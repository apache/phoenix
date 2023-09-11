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


import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

@Category(ParallelStatsDisabledTest.class)
public class IndexWithDefaultValueIT extends ParallelStatsDisabledIT {

    @Test
    public void testQueryTableWithIndex() throws Exception {
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();

        Properties props = new Properties();
        String schema = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl(), props);

        conn.setSchema(schema);
        conn.createStatement().execute("\n" +
                "create table " + tableName + "(\n" +
                "pk VARCHAR,\n" +
                "b VARCHAR,\n" +
                "c VARCHAR default '0',\n" +
                "CONSTRAINT my_pk PRIMARY KEY (pk)\n" +
                ")");

        conn.commit();

        conn.createStatement().execute("upsert into " + tableName + " values('1','1','1')");
        conn.commit();

        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(pk, b, c)");
        conn.commit();


        final PreparedStatement select = conn.prepareStatement(
                "select * from " + tableName);

        ResultSet rs = select.executeQuery();

        assertTrue(rs.next());
        assertEquals("1", rs.getString(3));
        assertFalse(rs.next());
        rs.close();
        conn.close();
    }



    @Test
    public void testQueryTableWithIndexBigintDefault() throws Exception {
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();

        Properties props = new Properties();
        String schema = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl(), props);


        conn.setSchema(schema);
        conn.createStatement().execute("\n" +
                "create table " + tableName + "(\n" +
                "id CHAR(32) NOT NULL,\n" +
                "no CHAR(32) default 'AB'," +
                "total BIGINT default 0,\n" +
                "score INTEGER default 0," +
                "CONSTRAINT my_pk PRIMARY KEY (id)\n" +
                ")");

        conn.commit();

        conn.createStatement().execute("upsert into " + tableName + "(id, no, total, score) values ('1111','1112', 1113, 1114)");
        conn.createStatement().execute("upsert into " + tableName + "(id, total) values ('1121', 1123)");
        conn.commit();

        conn.createStatement().execute("CREATE INDEX " + indexName + " on " + tableName + " (no, total, score)");
        conn.commit();


        final PreparedStatement select = conn.prepareStatement(
                "select * from " + tableName);

        ResultSet rs = select.executeQuery();

        assertTrue(rs.next());
        assertEquals(1113L, rs.getObject(3));
        assertEquals(1114, rs.getObject(4));
        assertTrue(rs.next());
        assertEquals("AB", rs.getObject(2));
        assertEquals(1123L, rs.getObject(3));
        assertEquals(0, rs.getObject(4));
        assertFalse(rs.next());

        rs.close();
        conn.close();
    }

    @Test
    public void testQueryTableWithIndexDefaultValue() throws Exception {
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();

        Properties props = new Properties();
        String schema = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl(), props);


        conn.setSchema(schema);
        conn.createStatement().execute("\n" +
                "create table " + tableName + "(\n" +
                "pk1 INTEGER NOT NULL, " +
                "pk2 INTEGER DEFAULT 10, " +
                "CONSTRAINT my_pk PRIMARY KEY (pk1)\n" +
                ")");

        conn.commit();

        conn.createStatement().execute("upsert into " + tableName + "(pk1, pk2) values (1,1)");
        conn.createStatement().execute("upsert into " + tableName + "(pk1, pk2) values (2, null)");
        conn.createStatement().execute("upsert into " + tableName + "(pk1) values (3)");
        conn.commit();

        conn.createStatement().execute("CREATE INDEX " + indexName + " on " + tableName + " (pk1, pk2)");
        conn.commit();


        final PreparedStatement select = conn.prepareStatement(
                "select * from " + tableName);

        ResultSet rs = select.executeQuery();

        assertTrue(rs.next());
        assertEquals(1, rs.getObject(1));
        assertEquals(1, rs.getObject(2));
        assertTrue(rs.next());
        assertEquals(2, rs.getObject(1));
        assertEquals(null, rs.getObject(2));
        assertTrue(rs.next());
        assertEquals(3, rs.getObject(1));
        assertEquals(10, rs.getObject(2));
        assertFalse(rs.next());

        rs.close();
        conn.close();
    }

    @Test
    public void testDefaultLocalIndexed() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "pk INTEGER PRIMARY KEY,"
                + "c1 INTEGER,"
                + "c2 INTEGER DEFAULT 100)";

        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl);
        conn.commit();

        String idx = generateUniqueName();
        ddl = "CREATE LOCAL INDEX " + idx + " on " + table + " (c2)";
        conn.createStatement().execute(ddl);
        conn.commit();

        String dml = "UPSERT INTO " + table + " (pk, c1) VALUES (1, 2)";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs =
                conn.createStatement().executeQuery("SELECT c2 FROM " + table + " WHERE c2 = 100");
        assertTrue(rs.next());
        assertEquals(100, rs.getInt(1));
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery("SELECT c2 FROM " + table + " WHERE c2 = 5");
        assertFalse(rs.next());
    }

    @Test
    public void testDefaultIndexed() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "pk INTEGER PRIMARY KEY,"
                + "c1 INTEGER,"
                + "c2 INTEGER DEFAULT 100)";

        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl);
        conn.commit();

        String idx = generateUniqueName();
        ddl = "CREATE INDEX " + idx + " on " + table + " (c2)";
        conn.createStatement().execute(ddl);
        conn.commit();

        String dml = "UPSERT INTO " + table + " (pk, c1) VALUES (1, 2)";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs =
                conn.createStatement().executeQuery("SELECT c2 FROM " + table + " WHERE c2 = 100");
        assertTrue(rs.next());
        assertEquals(100, rs.getInt(1));
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery("SELECT c2 FROM " + table + " WHERE c2 = 5");
        assertFalse(rs.next());
    }

    @Test
    public void testDefaultColumnValue() throws Exception {
        String sharedTable1 = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + sharedTable1 + " (" +
                "pk1 INTEGER NOT NULL, " +
                "pk2 INTEGER DEFAULT 10, " +
                "CONSTRAINT NAME_PK PRIMARY KEY (pk1))";

        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + sharedTable1 + " VALUES (1, 1)";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + sharedTable1 + " VALUES (2, null)";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + sharedTable1 + " VALUES (3)";
        conn.createStatement().execute(dml);
        conn.commit();


        String projection = "*";

        ResultSet rs = conn.createStatement()
                .executeQuery("SELECT " + projection + " FROM " + sharedTable1 + " WHERE pk1 = 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt(2));
        assertFalse(rs.next());

        rs = conn.createStatement()
                .executeQuery("SELECT " + projection + " FROM " + sharedTable1 + " WHERE pk1 = 2");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals(null, rs.getString(2));
        assertFalse(rs.next());

        rs = conn.createStatement()
                .executeQuery("SELECT " + projection + " FROM " + sharedTable1 + " WHERE pk1 = 3");
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals(10, rs.getInt(2));
        assertFalse(rs.next());
    }

}