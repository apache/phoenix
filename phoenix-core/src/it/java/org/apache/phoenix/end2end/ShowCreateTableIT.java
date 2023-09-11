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

import org.apache.phoenix.util.SchemaUtil;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

import static org.apache.phoenix.query.QueryConstants.DEFAULT_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE;
import static org.junit.Assert.assertTrue;

@Category(ParallelStatsDisabledTest.class)
public class ShowCreateTableIT extends ParallelStatsDisabledIT {
    @Test
    public void testShowCreateTableBasic() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + "(K VARCHAR NOT NULL PRIMARY KEY, INT INTEGER, INT2 INTEGER)";
        conn.createStatement().execute(ddl);

        ResultSet rs = conn.createStatement().executeQuery("SHOW CREATE TABLE " + tableName );
        assertTrue(rs.next());

        String expected = "CREATE TABLE " + tableName + "(K VARCHAR NOT NULL PRIMARY KEY, " +
                "INT INTEGER, INT2 INTEGER) IMMUTABLE_STORAGE_SCHEME='ONE_CELL_PER_COLUMN'";
        assertTrue("Expected: :" + expected + "\nResult: " + rs.getString(1),
                rs.getString(1).equals(expected));
    }

    @Test
    public void testShowCreateTableLowerCase() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = "lowercasetbl1";
        String ddl = "CREATE TABLE \"" + tableName + "\"(K VARCHAR NOT NULL PRIMARY KEY, INT INTEGER)";
        conn.createStatement().execute(ddl);

        ResultSet rs = conn.createStatement().executeQuery("SHOW CREATE TABLE \"" + tableName + "\"");
        assertTrue(rs.next());

        String expected = "CREATE TABLE \"" + tableName + "\"(K VARCHAR NOT NULL PRIMARY KEY, " +
                "INT INTEGER) IMMUTABLE_STORAGE_SCHEME='ONE_CELL_PER_COLUMN'";
        assertTrue("Expected: :" + expected + "\nResult: " + rs.getString(1),
                rs.getString(1).equals(expected));
    }

    @Test
    public void testShowCreateTableUpperCase() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String tableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String ddl = "CREATE TABLE " + tableFullName + "(K VARCHAR NOT NULL PRIMARY KEY, INT INTEGER)";
        conn.createStatement().execute(ddl);

        ResultSet rs = conn.createStatement().executeQuery("SHOW CREATE TABLE " + tableFullName);
        assertTrue(rs.next());

        String expected = "CREATE TABLE " + tableFullName + "(K VARCHAR NOT NULL PRIMARY KEY, " +
                "INT INTEGER) IMMUTABLE_STORAGE_SCHEME='ONE_CELL_PER_COLUMN'";
        assertTrue("Expected: :" + expected + "\nResult: " + rs.getString(1),
                rs.getString(1).equals(expected));
    }

    @Test
    public void testShowCreateTableDefaultFamily() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String ddl = "CREATE IMMUTABLE TABLE \"" + tableName + "\"(K VARCHAR NOT NULL PRIMARY KEY, " +
                "INT1 INTEGER, " +
                "INT2 INTEGER, " +
                "a.INT3 INTEGER, " +
                "\"A\".INT4 INTEGER, " +
                "\"b\".INT5 INTEGER, " +
                "\"B\".INT6 INTEGER) " +
                "DEFAULT_COLUMN_FAMILY='dF'";
        conn.createStatement().execute(ddl);

        ResultSet rs = conn.createStatement().executeQuery("SHOW CREATE TABLE " + tableName );
        assertTrue(rs.next());

        String expected = "CREATE TABLE " + tableName + "(K VARCHAR NOT NULL PRIMARY KEY, " +
                "\"dF\".INT1 INTEGER, \"dF\".INT2 INTEGER, A.INT3 INTEGER, A.INT4 INTEGER, " +
                "\"b\".INT5 INTEGER, B.INT6 INTEGER) " +
                "IMMUTABLE_ROWS=true, DEFAULT_COLUMN_FAMILY='dF'";
        assertTrue("Expected: :" + expected + "\nResult: " + rs.getString(1),
                rs.getString(1).equals(expected));
    }

    @Test
    public void testShowCreateTableDefaultFamilyNonConsecutive() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String ddl = "CREATE IMMUTABLE TABLE \"" + tableName + "\"(K VARCHAR NOT NULL PRIMARY KEY, " +
                "INT1 INTEGER, " +
                "INT2 INTEGER, " +
                "a.INT3 INTEGER, " +
                "\"A\".INT4 INTEGER, " +
                "\"b\".INT5 INTEGER, " +
                "\"B\".INT6 INTEGER) " +
                "DEFAULT_COLUMN_FAMILY='dF'";
        conn.createStatement().execute(ddl);

        String dropInt2 = "ALTER TABLE " + tableName + " DROP COLUMN INT2, INT4, INT5";
        conn.createStatement().execute(dropInt2);

        ResultSet rs = conn.createStatement().executeQuery("SHOW CREATE TABLE " + tableName );
        assertTrue(rs.next());

        String expected = "CREATE TABLE " + tableName + "(K VARCHAR NOT NULL PRIMARY KEY, " +
                "\"dF\".INT1 INTEGER ENCODED_QUALIFIER " + (ENCODED_CQ_COUNTER_INITIAL_VALUE) +
                ", A.INT3 INTEGER ENCODED_QUALIFIER " + (ENCODED_CQ_COUNTER_INITIAL_VALUE) +
                ", B.INT6 INTEGER) " +
                "IMMUTABLE_ROWS=true, DEFAULT_COLUMN_FAMILY='dF' COLUMN_QUALIFIER_COUNTER " +
                "('A'=" + (ENCODED_CQ_COUNTER_INITIAL_VALUE + 2) +
                ", 'dF'=" + (ENCODED_CQ_COUNTER_INITIAL_VALUE + 2) + ")";
        assertTrue("Expected: :" + expected + "\nResult: " + rs.getString(1),
                rs.getString(1).equals(expected));
    }

    @Test
    public void testShowCreateTableCounter() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + "(K VARCHAR NOT NULL PRIMARY KEY, " +
                "INT INTEGER ENCODED_QUALIFIER " + (ENCODED_CQ_COUNTER_INITIAL_VALUE) +
                ", INT2 INTEGER ENCODED_QUALIFIER " + (ENCODED_CQ_COUNTER_INITIAL_VALUE + 1) +
                ") IMMUTABLE_STORAGE_SCHEME='ONE_CELL_PER_COLUMN' COLUMN_QUALIFIER_COUNTER " +
                "('" + DEFAULT_COLUMN_FAMILY +"'=" + (ENCODED_CQ_COUNTER_INITIAL_VALUE + 3) + ")";
        conn.createStatement().execute(ddl);

        ResultSet rs = conn.createStatement().executeQuery("SHOW CREATE TABLE " + tableName );
        assertTrue(rs.next());
        assertTrue("Expected: :" + ddl + "\nResult: " + rs.getString(1),
                rs.getString(1).equals(ddl));
    }

    @Test
    public void testShowCreateTableColumnQualifierNonConsecutive() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + "(K VARCHAR NOT NULL PRIMARY KEY, " +
                "INT INTEGER, INT2 INTEGER, INT3 INTEGER)";
        conn.createStatement().execute(ddl);

        String dropInt2 = "ALTER TABLE " + tableName + " DROP COLUMN INT3";
        conn.createStatement().execute(dropInt2);

        ResultSet rs = conn.createStatement().executeQuery("SHOW CREATE TABLE \"" + tableName + "\"");
        assertTrue(rs.next());

        String expected = "CREATE TABLE " + tableName + "(K VARCHAR NOT NULL PRIMARY KEY, " +
                "INT INTEGER ENCODED_QUALIFIER " + (ENCODED_CQ_COUNTER_INITIAL_VALUE) +
                ", INT2 INTEGER ENCODED_QUALIFIER " + (ENCODED_CQ_COUNTER_INITIAL_VALUE + 1) +
                ") IMMUTABLE_STORAGE_SCHEME='ONE_CELL_PER_COLUMN' COLUMN_QUALIFIER_COUNTER " +
                "('" + DEFAULT_COLUMN_FAMILY +"'=" + (ENCODED_CQ_COUNTER_INITIAL_VALUE + 3) + ")";
        assertTrue("Expected: :" + expected + "\nResult: " + rs.getString(1),
                rs.getString(1).equals(expected));

        tableName = generateUniqueName();
        ddl = "CREATE TABLE " + tableName + "(K VARCHAR NOT NULL PRIMARY KEY, " +
                "INT INTEGER, INT2 INTEGER, INT3 INTEGER)";
        conn.createStatement().execute(ddl);

        dropInt2 = "ALTER TABLE " + tableName + " DROP COLUMN INT";
        conn.createStatement().execute(dropInt2);

        rs = conn.createStatement().executeQuery("SHOW CREATE TABLE \"" + tableName + "\"");
        assertTrue(rs.next());

        expected = "CREATE TABLE " + tableName + "(K VARCHAR NOT NULL PRIMARY KEY, " +
                "INT2 INTEGER ENCODED_QUALIFIER " + (ENCODED_CQ_COUNTER_INITIAL_VALUE + 1) +
                ", INT3 INTEGER ENCODED_QUALIFIER " + (ENCODED_CQ_COUNTER_INITIAL_VALUE + 2) +
                ") IMMUTABLE_STORAGE_SCHEME='ONE_CELL_PER_COLUMN'";
        assertTrue("Expected: :" + expected + "\nResult: " + rs.getString(1),
                rs.getString(1).equals(expected));

        tableName = generateUniqueName();
        ddl = "CREATE TABLE " + tableName + "(K VARCHAR NOT NULL PRIMARY KEY, " +
                "INT INTEGER, INT2 INTEGER, INT3 INTEGER)";
        conn.createStatement().execute(ddl);

        dropInt2 = "ALTER TABLE " + tableName + " DROP COLUMN INT2";
        conn.createStatement().execute(dropInt2);

        rs = conn.createStatement().executeQuery("SHOW CREATE TABLE \"" + tableName + "\"");
        assertTrue(rs.next());

        expected = "CREATE TABLE " + tableName + "(K VARCHAR NOT NULL PRIMARY KEY, " +
                "INT INTEGER ENCODED_QUALIFIER " + (ENCODED_CQ_COUNTER_INITIAL_VALUE) +
                ", INT3 INTEGER ENCODED_QUALIFIER " + (ENCODED_CQ_COUNTER_INITIAL_VALUE + 2) +
                ") IMMUTABLE_STORAGE_SCHEME='ONE_CELL_PER_COLUMN'";
        assertTrue("Expected: :" + expected + "\nResult: " + rs.getString(1),
                rs.getString(1).equals(expected));
    }

    @Test
    public void testShowCreateTableColumnQualifierDropAndAdd() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();;
        String ddl = "CREATE TABLE " + tableName + "(K VARCHAR NOT NULL PRIMARY KEY, " +
                "INT INTEGER, INT2 INTEGER, INT3 INTEGER)";
        conn.createStatement().execute(ddl);

        String dropInt3 = "ALTER TABLE " + tableName + " DROP COLUMN INT3";
        conn.createStatement().execute(dropInt3);

        String addInt4 = "ALTER TABLE " + tableName + " ADD INT4 INTEGER";
        conn.createStatement().execute(addInt4);

        ResultSet rs = conn.createStatement().executeQuery("SHOW CREATE TABLE \"" + tableName + "\"");
        assertTrue(rs.next());

        String expected = "CREATE TABLE " + tableName + "(K VARCHAR NOT NULL PRIMARY KEY, " +
                "INT INTEGER ENCODED_QUALIFIER " + (ENCODED_CQ_COUNTER_INITIAL_VALUE) +
                ", INT2 INTEGER ENCODED_QUALIFIER " + (ENCODED_CQ_COUNTER_INITIAL_VALUE + 1) +
                ", INT4 INTEGER ENCODED_QUALIFIER " + (ENCODED_CQ_COUNTER_INITIAL_VALUE + 3) +
                ") IMMUTABLE_STORAGE_SCHEME='ONE_CELL_PER_COLUMN'";
        assertTrue("Expected: :" + expected + "\nResult: " + rs.getString(1),
                rs.getString(1).equals(expected));
    }

    @Test
    public void testShowCreateTableColumnQualifierMultipleFamilies() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();;
        String ddl = "CREATE IMMUTABLE TABLE " + tableName +
                "(K VARCHAR NOT NULL PRIMARY KEY, A.INT INTEGER, B.INT2 INTEGER) " +
                "IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS";
        conn.createStatement().execute(ddl);
        ResultSet rs = conn.createStatement().executeQuery("SHOW CREATE TABLE \"" + tableName + "\"");
        assertTrue(rs.next());

        String expected = "CREATE TABLE " + tableName + "(K VARCHAR NOT NULL PRIMARY KEY, " +
                "A.INT INTEGER, B.INT2 INTEGER) IMMUTABLE_ROWS=true";
        assertTrue("Expected: :" + expected + "\nResult: " + rs.getString(1),
                rs.getString(1).equals(expected));
    }

    @Test
    public void testShowCreateTableColumnQualifierMultipleFamiliesNonConsecutive() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();;
        String ddl = "CREATE IMMUTABLE TABLE " + tableName +
                "(K VARCHAR NOT NULL PRIMARY KEY, A.INT INTEGER, A.INT2 INTEGER, " +
                "B.INT3 INTEGER, B.INT4 INTEGER) " +
                "IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS";
        conn.createStatement().execute(ddl);

        String dropInt = "ALTER TABLE " + tableName + " DROP COLUMN INT4";
        conn.createStatement().execute(dropInt);

        ResultSet rs = conn.createStatement().executeQuery("SHOW CREATE TABLE \"" + tableName + "\"");
        assertTrue(rs.next());

        String expected = "CREATE TABLE " + tableName + "(K VARCHAR NOT NULL PRIMARY KEY, " +
                "A.INT INTEGER, A.INT2 INTEGER, B.INT3 INTEGER ENCODED_QUALIFIER " +
                (ENCODED_CQ_COUNTER_INITIAL_VALUE) + ") IMMUTABLE_ROWS=true " +
                "COLUMN_QUALIFIER_COUNTER ('B'=" + (ENCODED_CQ_COUNTER_INITIAL_VALUE + 2) + ")";
        assertTrue("Expected: :" + expected + "\nResult: " + rs.getString(1),
                rs.getString(1).equals(expected));


        tableName = generateUniqueName();;
        ddl = "CREATE IMMUTABLE TABLE " + tableName +
                "(K VARCHAR NOT NULL PRIMARY KEY, A.INT INTEGER, A.INT2 INTEGER, " +
                "B.INT3 INTEGER, B.INT4 INTEGER) " +
                "IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS";
        conn.createStatement().execute(ddl);

        dropInt = "ALTER TABLE " + tableName + " DROP COLUMN INT2, INT3";
        conn.createStatement().execute(dropInt);

        rs = conn.createStatement().executeQuery("SHOW CREATE TABLE \"" + tableName + "\"");
        assertTrue(rs.next());

        expected = "CREATE TABLE " + tableName + "(K VARCHAR NOT NULL PRIMARY KEY, " +
                "A.INT INTEGER ENCODED_QUALIFIER " + (ENCODED_CQ_COUNTER_INITIAL_VALUE) +
                ", B.INT4 INTEGER ENCODED_QUALIFIER " +
                (ENCODED_CQ_COUNTER_INITIAL_VALUE + 1) + ") IMMUTABLE_ROWS=true " +
                "COLUMN_QUALIFIER_COUNTER ('A'=" + (ENCODED_CQ_COUNTER_INITIAL_VALUE + 2) + ")";
        assertTrue("Expected: :" + expected + "\nResult: " + rs.getString(1),
                rs.getString(1).equals(expected));
    }

    @Test
    public void testShowCreateTableView() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String viewName = generateUniqueName();

        String schemaName = generateUniqueName();
        String tableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String viewFullName = SchemaUtil.getQualifiedTableName(schemaName, viewName);
        String ddl = "CREATE TABLE " + tableFullName + "(K VARCHAR NOT NULL PRIMARY KEY, INT INTEGER)";
        conn.createStatement().execute(ddl);
        String ddlView = "CREATE VIEW " + viewFullName + " AS SELECT * FROM " + tableFullName;
        conn.createStatement().execute(ddlView);

        ResultSet rs = conn.createStatement().executeQuery("SHOW CREATE TABLE " + viewFullName);
        assertTrue(rs.next());
        assertTrue("Expected: :" + ddlView + "\nResult: " + rs.getString(1),
                rs.getString(1).contains(ddlView));
    }

    @Test
    public void testShowCreateTableIndex() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String indexname = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + "(K VARCHAR NOT NULL PRIMARY KEY, INT INTEGER)";
        conn.createStatement().execute(ddl);
        String createIndex = "CREATE INDEX " + indexname + " ON " + tableName + "(K DESC)";
        conn.createStatement().execute(createIndex);
        ResultSet rs = conn.createStatement().executeQuery("SHOW CREATE TABLE " + indexname);
        assertTrue(rs.next());
        assertTrue("Expected: " + createIndex + "\nResult: " + rs.getString(1),
                rs.getString(1).contains(createIndex));
    }
}
