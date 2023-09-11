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
package org.apache.phoenix.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.junit.Test;

public class PhoenixResultSetMetadataTest extends BaseConnectionlessQueryTest {
    
    @Test
    public void testColumnDisplaySize() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(
                "CREATE TABLE T (pk1 CHAR(15) not null, pk2 VARCHAR not null,  v1 VARCHAR(15), v2 DATE, v3 VARCHAR " +
                "CONSTRAINT pk PRIMARY KEY (pk1, pk2)) ");
        ResultSet rs = conn.createStatement().executeQuery("SELECT pk1, pk2, v1, v2, CAST(null AS varchar) FROM T");
        assertEquals(15, rs.getMetaData().getColumnDisplaySize(1));
        assertEquals(PhoenixResultSetMetaData.DEFAULT_DISPLAY_WIDTH, rs.getMetaData().getColumnDisplaySize(2));
        assertEquals(15, rs.getMetaData().getColumnDisplaySize(3));
        assertEquals(conn.unwrap(PhoenixConnection.class).getDatePattern().length(), rs.getMetaData().getColumnDisplaySize(4));
        assertEquals(40, rs.getMetaData().getColumnDisplaySize(5));
    }

    @Test
    public void testNullTypeName() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("select null");

        assertEquals("NULL", rs.getMetaData().getColumnTypeName(1));
    }

    @Test
    public void testCaseSensitiveExpression() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(
                "CREATE TABLE T (pk1 CHAR(15) not null, pk2 VARCHAR not null,  \"v1\" VARCHAR(15), v2 DATE, \"v3\" VARCHAR " +
                        "CONSTRAINT pk PRIMARY KEY (pk1, pk2)) ");
        ResultSet rs = conn.createStatement().executeQuery("SELECT pk1 AS testalias1, pk2 AS \"testalias2\", " +
                "\"v1\" AS \"testalias3\", v2, \"v3\" FROM T");

        assertEquals("PK1", rs.getMetaData().getColumnName(1));
        assertEquals("TESTALIAS1", rs.getMetaData().getColumnLabel(1));
        assertFalse(rs.getMetaData().isCaseSensitive(1));

        assertEquals("PK2", rs.getMetaData().getColumnName(2));
        assertEquals("testalias2", rs.getMetaData().getColumnLabel(2));
        assertTrue(rs.getMetaData().isCaseSensitive(2));

        assertEquals("v1", rs.getMetaData().getColumnName(3));
        assertEquals("testalias3", rs.getMetaData().getColumnLabel(3));
        assertTrue(rs.getMetaData().isCaseSensitive(3));

        assertEquals("V2", rs.getMetaData().getColumnName(4));
        assertEquals("V2", rs.getMetaData().getColumnLabel(4));
        assertFalse(rs.getMetaData().isCaseSensitive(4));

        assertEquals("v3", rs.getMetaData().getColumnName(5));
        assertEquals("v3", rs.getMetaData().getColumnLabel(5));
        assertTrue(rs.getMetaData().isCaseSensitive(5));
    }

    @Test
    public void testLabel() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(
                "CREATE TABLE T (pk1 CHAR(15) not null, pk2 VARCHAR not null,  v1 VARCHAR(15), v2 DATE, v3 VARCHAR " +
                        "CONSTRAINT pk PRIMARY KEY (pk1, pk2)) ");
        ResultSet rs = conn.createStatement().executeQuery("SELECT pk1 AS testalias1, pk2, " +
                "v1 AS testalias2, v2 FROM T");
        assertEquals("PK1", rs.getMetaData().getColumnName(1));
        assertEquals("TESTALIAS1", rs.getMetaData().getColumnLabel(1));
        assertEquals("PK2", rs.getMetaData().getColumnName(2));
        assertEquals("PK2", rs.getMetaData().getColumnLabel(2));
        assertEquals("V1", rs.getMetaData().getColumnName(3));
        assertEquals("TESTALIAS2", rs.getMetaData().getColumnLabel(3));
        assertEquals("V2", rs.getMetaData().getColumnName(4));
        assertEquals("V2", rs.getMetaData().getColumnLabel(4));
    }

    @Test
    public void testSummandExpression() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(
                "CREATE TABLE T (pk1 CHAR(15) not null, pk2 INTEGER not null,  v1 VARCHAR(15), v2 DATE, v3 VARCHAR " +
                        "CONSTRAINT pk PRIMARY KEY (pk1, pk2)) ");
        ResultSet rs = conn.createStatement().executeQuery("SELECT 3+pk2 FROM T");
        assertEquals("(3 + PK2)", rs.getMetaData().getColumnName(1));
        assertEquals("(3 + PK2)", rs.getMetaData().getColumnLabel(1));
        rs = conn.createStatement().executeQuery("SELECT 3+pk2 AS sum FROM T");
        assertEquals("(3 + PK2)", rs.getMetaData().getColumnName(1));
        assertEquals("SUM", rs.getMetaData().getColumnLabel(1));
    }

    @Test
    public void testSqrtExpression() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(
                "CREATE TABLE T (pk1 CHAR(15) not null, pk2 INTEGER not null,  v1 VARCHAR(15), v2 DATE, v3 VARCHAR " +
                        "CONSTRAINT pk PRIMARY KEY (pk1, pk2)) ");
        ResultSet rs = conn.createStatement().executeQuery("SELECT SQRT(3+pk2) FROM T");
        assertEquals("SQRT((3 + PK2))", rs.getMetaData().getColumnName(1));
        assertEquals("SQRT((3 + PK2))", rs.getMetaData().getColumnLabel(1));
        rs = conn.createStatement().executeQuery("SELECT SQRT(3+pk2) AS \"sqrt\" FROM T");
        assertEquals("SQRT((3 + PK2))", rs.getMetaData().getColumnName(1));
        assertEquals("sqrt", rs.getMetaData().getColumnLabel(1));
    }

    @Test
    public void testView() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS S.T (A INTEGER PRIMARY KEY, B INTEGER, C VARCHAR, D INTEGER)");
        conn.createStatement().execute(
                "CREATE VIEW IF NOT EXISTS S.V (VA INTEGER, VB INTEGER) AS SELECT * FROM S.T WHERE B=200");
        conn.createStatement().execute(
                "UPSERT INTO S.V (A, B, C, D, VA, VB) VALUES (2, 200, 'def', -20, 91, 101)");
        conn.createStatement().execute(
                "ALTER VIEW S.V DROP COLUMN C");

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM S.V");
        assertEquals("A", rs.getMetaData().getColumnName(1));
        assertEquals("A", rs.getMetaData().getColumnLabel(1));
        assertEquals("B", rs.getMetaData().getColumnName(2));
        assertEquals("B", rs.getMetaData().getColumnLabel(2));
        assertEquals("C", rs.getMetaData().getColumnName(3));
        assertEquals("C", rs.getMetaData().getColumnLabel(3));
        assertEquals("D", rs.getMetaData().getColumnName(4));
        assertEquals("D", rs.getMetaData().getColumnLabel(4));
        assertEquals("VA", rs.getMetaData().getColumnName(5));
        assertEquals("VA", rs.getMetaData().getColumnLabel(5));
        assertEquals("VB", rs.getMetaData().getColumnName(6));
        assertEquals("VB", rs.getMetaData().getColumnLabel(6));
    }
}
