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

import static java.sql.Types.BIGINT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_ENCODED_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_STORAGE_SCHEME;
import static org.apache.phoenix.schema.PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN;
import static org.junit.Assert.assertEquals;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.apache.phoenix.query.QueryServices.WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB;

/**
 * Tests to check whether we correctly expose dynamic columns for wildcard queries when
 * {@link org.apache.phoenix.query.QueryServices#WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB} config is
 * turned on
 */
//FIXME this class has no @Category and is never run by maven
@RunWith(Parameterized.class)
public class DynamicColumnWildcardIT extends BaseTest {
    private final boolean mutableTable;
    private final ImmutableStorageScheme storageScheme;

    // name is used by failsafe as file name in reports
    @Parameterized.Parameters(name="DynamicColumnWildcardIT_mutable={0}, storageScheme={1}")
    public static Collection<Object[]> data() {
        // TODO: Once PHOENIX-5107 is fixed, add a case for SINGLE_CELL_ARRAY_WITH_OFFSETS
        return Arrays.asList(new Object[][] {
                {true, null}, {false, ONE_CELL_PER_COLUMN}});
    }

    public DynamicColumnWildcardIT(boolean mutableTable, ImmutableStorageScheme storageScheme) {
        this.mutableTable = mutableTable;
        this.storageScheme = storageScheme;
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(1);
        clientProps.put(WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB, "true");
        setUpTestDriver(ReadOnlyProps.EMPTY_PROPS, new ReadOnlyProps(clientProps));
    }

    // Create either a mutable table or an immutable table with the specified storage scheme
    private String generateTableCreateDDL(String tableName, String schema) {
        StringBuilder sb = new StringBuilder("CREATE ");
        if (!this.mutableTable) {
            sb.append("IMMUTABLE ");
        }
        sb.append("TABLE ").append(tableName).append(schema);
        if (!this.mutableTable && this.storageScheme != null) {
            sb.append(" ").append(IMMUTABLE_STORAGE_SCHEME).append("=").append(this.storageScheme);
            sb.append(", ").append(COLUMN_ENCODED_BYTES).append("=1");
        }
        return sb.toString();
    }

    @Test
    // Test the case where the table DDL only contains 1 column which is the primary key
    public void testOnlySinglePkWithDynamicColumns() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(true);
        String tableName = generateUniqueName();
        conn.createStatement().execute(generateTableCreateDDL(tableName,
                " (A INTEGER PRIMARY KEY)"));
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (A) VALUES(10)");
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (A, DYN1 INTEGER) VALUES(90, 3)");
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (A, DYN1 VARCHAR) VALUES(100, 'test')");

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
        int rsCounter = 0;
        while(rs.next()) {
            ResultSetMetaData rmd = rs.getMetaData();
            int count = rmd.getColumnCount();
            assertEquals(rsCounter == 0 ? 1 : 2, count);
            for (int i = 1; i <= count; i++) {
                if (rsCounter == 0) {
                    assertEquals("A", rmd.getColumnName(i));
                    assertEquals(INTEGER, rmd.getColumnType(i));
                    assertEquals(10, rs.getObject(i));
                } else if (rsCounter == 1) {
                    if (i == 1) {
                        assertEquals("A", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(90, rs.getObject(i));
                    } else if (i == 2) {
                        assertEquals("DYN1", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(3, rs.getObject(i));
                    }
                } else if (rsCounter == 2) {
                    if (i == 1) {
                        assertEquals("A", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(100, rs.getObject(i));
                    } else if (i ==2) {
                        assertEquals("DYN1", rmd.getColumnName(i));
                        assertEquals(VARCHAR, rmd.getColumnType(i));
                        assertEquals("test", rs.getObject(i));
                    }
                }
            }
            rsCounter++;
        }
    }

    @Test
    // Test the case where the table DDL contains 1 primary key column and other columns as well
    public void testSinglePkAndOtherColsWithDynamicColumns() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(true);
        String tableName = generateUniqueName();

        conn.createStatement().execute(generateTableCreateDDL(tableName,
                " (A INTEGER PRIMARY KEY, B VARCHAR)"));
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (A, B) VALUES(10, 'test1')");
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (A, B, DYN1 INTEGER) VALUES(90, 'test2', 3)");
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (A, DYN1 INTEGER, DYN2 VARCHAR) VALUES(100, 5, 'test3')");

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
        int rsCounter = 0;
        while(rs.next()) {
            ResultSetMetaData rmd = rs.getMetaData();
            int count = rmd.getColumnCount();
            assertEquals(rsCounter == 0 ? 2 : rsCounter == 1 ? 3 : 4,
                    count);
            for (int i = 1; i <= count; i++) {
                if (rsCounter == 0) {
                    if (i == 1) {
                        assertEquals("A", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(10, rs.getObject(i));
                    } else if (i == 2) {
                        assertEquals("B", rmd.getColumnName(i));
                        assertEquals(VARCHAR, rmd.getColumnType(i));
                        assertEquals("test1", rs.getObject(i));
                    }
                } else if (rsCounter == 1) {
                    if (i == 1) {
                        assertEquals("A", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(90, rs.getObject(i));
                    } else if (i == 2) {
                        assertEquals("B", rmd.getColumnName(i));
                        assertEquals(VARCHAR, rmd.getColumnType(i));
                        assertEquals("test2", rs.getObject(i));
                    } else if (i == 3) {
                        assertEquals("DYN1", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(3, rs.getObject(i));
                    }
                } else if (rsCounter == 2) {
                    if (i == 1) {
                        assertEquals("A", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(100, rs.getObject(i));
                    } else if (i == 2) {
                        assertEquals("B", rmd.getColumnName(i));
                        assertEquals(VARCHAR, rmd.getColumnType(i));
                        // Note that we didn't upsert any value for column 'B' so we should get null
                        assertEquals(null, rs.getObject(i));
                    } else if (i == 3) {
                        assertEquals("DYN1", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(5, rs.getObject(i));
                    } else if (i == 4) {
                        assertEquals("DYN2", rmd.getColumnName(i));
                        assertEquals(VARCHAR, rmd.getColumnType(i));
                        assertEquals("test3", rs.getObject(i));
                    }
                }
            }
            rsCounter++;
        }
    }

    @Test
    // Test the case where the table DDL contains just the composite key and no other columns
    public void testCompositeKeyWithDynamicColumns() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(true);
        String tableName = generateUniqueName();

        conn.createStatement().execute(generateTableCreateDDL(tableName,
                " (A INTEGER NOT NULL, B INTEGER NOT NULL CONSTRAINT PK PRIMARY KEY (A, B))"));
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (A, B) VALUES(10, 500)");
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (A, B, DYN1 INTEGER) VALUES(90, 100, 3)");
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (A, B, DYN2 VARCHAR) VALUES(999, 50, 'test1')");

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
        int rsCounter = 0;
        while(rs.next()) {
            ResultSetMetaData rmd = rs.getMetaData();
            int count = rmd.getColumnCount();
            assertEquals(rsCounter == 0 ? 2 : 3, count);
            for (int i = 1; i <= count; i++) {
                if (rsCounter == 0) {
                    if (i == 1) {
                        assertEquals("A", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(10, rs.getObject(i));
                    } else if (i == 2) {
                        assertEquals("B", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(500, rs.getObject(i));
                    }
                } else if (rsCounter == 1) {
                    if (i == 1) {
                        assertEquals("A", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(90, rs.getObject(i));
                    } else if (i == 2) {
                        assertEquals("B", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(100, rs.getObject(i));
                    } else if (i == 3) {
                        assertEquals("DYN1", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(3, rs.getObject(i));
                    }
                } else if (rsCounter == 2) {
                    if (i == 1) {
                        assertEquals("A", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(999, rs.getObject(i));
                    } else if (i == 2) {
                        assertEquals("B", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(50, rs.getObject(i));
                    } else if (i == 3) {
                        assertEquals("DYN2", rmd.getColumnName(i));
                        assertEquals(VARCHAR, rmd.getColumnType(i));
                        assertEquals("test1", rs.getObject(i));
                    }
                }
            }
            rsCounter++;
        }
    }

    @Test
    // Test the case where the table DDL contains the composite key and other columns
    public void testCompositeKeyAndOtherColsWithDynamicColumns() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(true);
        String tableName = generateUniqueName();

        conn.createStatement().execute(generateTableCreateDDL(tableName,
                " (A INTEGER NOT NULL, B INTEGER NOT NULL, C VARCHAR" +
                " CONSTRAINT PK PRIMARY KEY (A, B))"));
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (A, B) VALUES(10, 500)");
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (A, B, C) VALUES(20, 7, 'test1')");
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (A, B, DYN1 INTEGER) VALUES(30, 100, 3)");
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (A, B, C, DYN2 VARCHAR, DYN3 BIGINT) VALUES(40, 60, 'test1', 'test2', 8)");

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
        int rsCounter = 0;
        while(rs.next()) {
            ResultSetMetaData rmd = rs.getMetaData();
            int count = rmd.getColumnCount();
            assertEquals(rsCounter <= 1 ?
                    3 : rsCounter == 2 ? 4 : 5, count);
            for (int i = 1; i <= count; i++) {
                if (rsCounter == 0) {
                    if (i == 1) {
                        assertEquals("A", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(10, rs.getObject(i));
                    } else if (i == 2) {
                        assertEquals("B", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(500, rs.getObject(i));
                    } else if (i == 3) {
                        assertEquals("C", rmd.getColumnName(i));
                        assertEquals(VARCHAR, rmd.getColumnType(i));
                        assertEquals(null, rs.getObject(i));
                    }
                } else if (rsCounter == 1) {
                    if (i == 1) {
                        assertEquals("A", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(20, rs.getObject(i));
                    } else if (i == 2) {
                        assertEquals("B", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(7, rs.getObject(i));
                    } else if (i == 3) {
                        assertEquals("C", rmd.getColumnName(i));
                        assertEquals(VARCHAR, rmd.getColumnType(i));
                        assertEquals("test1", rs.getObject(i));
                    }
                } else if (rsCounter == 2) {
                    if (i == 1) {
                        assertEquals("A", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(30, rs.getObject(i));
                    } else if (i == 2) {
                        assertEquals("B", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(100, rs.getObject(i));
                    } else if (i == 3) {
                        assertEquals("C", rmd.getColumnName(i));
                        assertEquals(VARCHAR, rmd.getColumnType(i));
                        assertEquals(null, rs.getObject(i));
                    } else if (i == 4) {
                        assertEquals("DYN1", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(3, rs.getObject(i));
                    }
                } else if (rsCounter == 3) {
                    if (i == 1) {
                        assertEquals("A", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(40, rs.getObject(i));
                    } else if (i == 2) {
                        assertEquals("B", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(60, rs.getObject(i));
                    } else if (i == 3) {
                        assertEquals("C", rmd.getColumnName(i));
                        assertEquals(VARCHAR, rmd.getColumnType(i));
                        assertEquals("test1", rs.getObject(i));
                    } else if (i == 4) {
                        assertEquals("DYN2", rmd.getColumnName(i));
                        assertEquals(VARCHAR, rmd.getColumnType(i));
                        assertEquals("test2", rs.getObject(i));
                    } else if (i == 5) {
                        assertEquals("DYN3", rmd.getColumnName(i));
                        assertEquals(BIGINT, rmd.getColumnType(i));
                        assertEquals(8L, rs.getObject(i));
                    }
                }
            }
            rsCounter++;
        }
    }

    @Test
    // Test if dynamic columns are properly exposed in column family wildcard queries
    public void testColumnFamilyWildcards() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(true);
        String tableName = generateUniqueName();

        conn.createStatement().execute(generateTableCreateDDL(tableName,
                " (A INTEGER PRIMARY KEY, B VARCHAR, CF1.C INTEGER, CF2.D VARCHAR)"));
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (A, B, C, D) VALUES(10, 'test1', 2, 'test2')");
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (A, B, C, D, DYN0 INTEGER) VALUES(20, 'test3', 4, 'test4', 100)");
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (A, B, C, D, CF1.DYN1 VARCHAR, CF1.DYN2 INTEGER)" +
                " VALUES(30, 'test5', 5, 'test6', 'test7', 70)");
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (A, B, C, D, CF2.DYN1 VARCHAR, CF2.DYN2 INTEGER)" +
                " VALUES(40, 'test8', 6, 'test9', 'test10', 80)");
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (A, B, C, D, CF1.DYN3 VARCHAR, CF2.DYN4 INTEGER)" +
                " VALUES(50, 'test11', 7, 'test12', 'test13', 90)");

        ResultSet rs = conn.createStatement().executeQuery("SELECT CF1.* FROM " + tableName);
        int rsCounter = 0;
        while(rs.next()) {
            ResultSetMetaData rmd = rs.getMetaData();
            int count = rmd.getColumnCount();
            assertEquals(rsCounter <= 1 || rsCounter == 3 ?
                    1 : rsCounter == 2 ? 3 : 2, count);
            for (int i = 1; i <= count; i++) {
                if (rsCounter == 0) {
                    if (i == 1) {
                        assertEquals("C", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(2, rs.getObject(i));
                    }
                } else if (rsCounter == 1) {
                    if (i == 1) {
                        assertEquals("C", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(4, rs.getObject(i));
                    }
                } else if (rsCounter == 2) {
                    if (i == 1) {
                        assertEquals("C", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(5, rs.getObject(i));
                    } else if (i == 2) {
                        assertEquals("DYN1", rmd.getColumnName(i));
                        assertEquals(VARCHAR, rmd.getColumnType(i));
                        assertEquals("test7", rs.getObject(i));
                    } else if (i == 3) {
                        assertEquals("DYN2", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(70, rs.getObject(i));
                    }
                } else if (rsCounter == 3) {
                    if (i == 1) {
                        assertEquals("C", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(6, rs.getObject(i));
                    }
                } else if (rsCounter == 4) {
                    if (i == 1) {
                        assertEquals("C", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(7, rs.getObject(i));
                    } else if (i == 2) {
                        assertEquals("DYN3", rmd.getColumnName(i));
                        assertEquals(VARCHAR, rmd.getColumnType(i));
                        assertEquals("test13", rs.getObject(i));
                    }
                }
            }
            rsCounter++;
        }

        rs = conn.createStatement().executeQuery("SELECT CF2.* FROM " + tableName);
        rsCounter = 0;
        while(rs.next()) {
            ResultSetMetaData rmd = rs.getMetaData();
            int count = rmd.getColumnCount();
            assertEquals(rsCounter <= 2 ?
                    1 : rsCounter == 3 ? 3 : 2, count);
            for (int i = 1; i <= count; i++) {
                if (rsCounter == 0) {
                    if (i == 1) {
                        assertEquals("D", rmd.getColumnName(i));
                        assertEquals(VARCHAR, rmd.getColumnType(i));
                        assertEquals("test2", rs.getObject(i));
                    }
                } else if (rsCounter == 1) {
                    if (i == 1) {
                        assertEquals("D", rmd.getColumnName(i));
                        assertEquals(VARCHAR, rmd.getColumnType(i));
                        assertEquals("test4", rs.getObject(i));
                    }
                } else if (rsCounter == 2) {
                    if (i == 1) {
                        assertEquals("D", rmd.getColumnName(i));
                        assertEquals(VARCHAR, rmd.getColumnType(i));
                        assertEquals("test6", rs.getObject(i));
                    }
                } else if (rsCounter == 3) {
                    if (i == 1) {
                        assertEquals("D", rmd.getColumnName(i));
                        assertEquals(VARCHAR, rmd.getColumnType(i));
                        assertEquals("test9", rs.getObject(i));
                    } else if (i == 2) {
                        assertEquals("DYN1", rmd.getColumnName(i));
                        assertEquals(VARCHAR, rmd.getColumnType(i));
                        assertEquals("test10", rs.getObject(i));
                    } else if (i == 3) {
                        assertEquals("DYN2", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(80, rs.getObject(i));
                    }
                } else if (rsCounter == 4) {
                    if (i == 1) {
                        assertEquals("D", rmd.getColumnName(i));
                        assertEquals(VARCHAR, rmd.getColumnType(i));
                        assertEquals("test12", rs.getObject(i));
                    } else if (i == 2) {
                        assertEquals("DYN4", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(90, rs.getObject(i));
                    }
                }
            }
            rsCounter++;
        }

    }

}
