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

import static junit.framework.TestCase.assertTrue;
import static org.apache.phoenix.query.BaseTest.generateUniqueName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.phoenix.query.BaseTest;
import org.junit.Test;

public class Unnest2IT extends ParallelStatsDisabledIT{
    protected static String createTableWithArray(String url, byte[][] bs, Object object) throws
            SQLException {
        String tableName = generateUniqueName();
        String ddlStmt = "create table "
                + tableName
                + "   (id char(15) not null, \n"
                + "    a_int integer,\n"
                + "    a_double_array double array,\n"
                + "    a_varchar_array varchar(100) array,\n"
                + "    CONSTRAINT pk PRIMARY KEY (id)\n"
                + ")";
        BaseTest.createTestTable(url, ddlStmt, bs, null);
        return tableName;
    }
    protected static void initTablesWithArrays(String tableName, String tennantId, String url) throws Exception{
        Properties props = new Properties();
        String query =
                "upsert into " + tableName + "("
                        + "    id, "
                        + "    a_int, "
                        + "    a_double_array, "
                        + "    a_varchar_array) "
                        + "VALUES (?, ?, ?, ?)";

        try(Connection conn = DriverManager.getConnection(url, props)) {
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1,"N0001");
            stmt.setInt(2,35);
            Double[] doubleArray = {24d,34d,98d,29d};
            Array array = conn.createArrayOf("DOUBLE",doubleArray);
            stmt.setArray(3,array);
            String[] strArray = {"XX","YYY","BBB","DDD","XX","WWW"};
            array = conn.createArrayOf("VARCHAR",strArray);
            stmt.setArray(4,array);
            stmt.execute();
            conn.commit();
            stmt = conn.prepareStatement(query);
            stmt.setString(1,"N0002");
            stmt.setInt(2,74);
            doubleArray = new Double[]{22d,77d,25d,26d};
            array = conn.createArrayOf("DOUBLE",doubleArray);
            stmt.setArray(3,array);
            strArray = new String[]{"YYY","XX","XX","YYY","XX"};
            array = conn.createArrayOf("VARCHAR",strArray);
            stmt.setArray(4,array);
            stmt.execute();
            conn.commit();
        }
    }

    @Test
    public void testCountUnnestGroupByUnnest() throws Exception{
        Properties props = new Properties();
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, getUrl());
        try(Connection conn = DriverManager.getConnection(url, props)){
            String query =
                    "SELECT COUNT(id) AS A,UNNEST(a_varchar_array) AS B\n"
                    + "FROM " + tableName +"\n"
                    + "GROUP BY B";
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            //System.out.println(rs.get);
            assertEquals(1,rs.getInt("A"));
            assertEquals("BBB",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(1,rs.getInt("A"));
            assertEquals("DDD",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(1,rs.getInt("A"));
            assertEquals("WWW",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(5,rs.getInt("A"));
            assertEquals("XX",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(3,rs.getInt("A"));
            assertEquals("YYY",rs.getString("B"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testCountUnnestGroupByUnnestOrderByUnnest() throws Exception{
        Properties props = new Properties();
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, getUrl());
        try(Connection conn = DriverManager.getConnection(url, props)){
            String query = "SELECT COUNT(1) AS A,UNNEST(a_varchar_array) AS B\n"
                    + "FROM " + tableName + "\n"
                    + "GROUP BY B\n"
                    + "ORDER BY B";
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(1,rs.getInt("A"));
            assertEquals("BBB",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(1,rs.getInt("A"));
            assertEquals("DDD",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(1,rs.getInt("A"));
            assertEquals("WWW",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(5,rs.getInt("A"));
            assertEquals("XX",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(3,rs.getInt("A"));
            assertEquals("YYY",rs.getString("B"));
            assertFalse(rs.next());
        }
    }
    @Test
    public void testCountUnnestGroupByUnnestHavingCountUnnest() throws Exception{
        Properties props = new Properties();
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, getUrl());
        try(Connection conn = DriverManager.getConnection(url, props)){
            String query =
                    "SELECT COUNT(UNNEST(a_varchar_array)) AS A, UNNEST(a_varchar_array) AS B\n"
                    + "FROM " + tableName +"\n"
                    + "GROUP BY B\n"
                    + "HAVING COUNT(UNNEST(a_varchar_array)) > 2";
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(5,rs.getInt("A"));
            assertEquals("XX",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(3,rs.getInt("A"));
            assertEquals("YYY",rs.getString("B"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testUnnestOnDynamicColumn()throws Exception{
        Properties props = new Properties();
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, getUrl());

        try(Connection conn = DriverManager.getConnection(url, props)){
            String query =
                    "UPSERT INTO " + tableName + "(id, b_varchar_array VARCHAR(100) ARRAY) "
                    + "VALUES('qwert', ARRAY['dd','ee','ff'])";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            query = "SELECT UNNEST(b_varchar_array) AS A FROM " + tableName + "(b_varchar_array VARCHAR(100) ARRAY)";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("dd",rs.getString("A"));
            assertTrue(rs.next());
            assertEquals("ee",rs.getString("A"));
            assertTrue(rs.next());
            assertEquals("ff",rs.getString("A"));
            assertFalse(rs.next());

        }
    }

    @Test
    public void testUnnestStaticAndDynamicColumn()throws Exception{
        Properties props = new Properties();
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, getUrl());

        try(Connection conn = DriverManager.getConnection(url, props)){
            String query =
                    "UPSERT INTO " + tableName + "(id,a_varchar_array, b_varchar_array VARCHAR(100) ARRAY) "
                            + "VALUES('qwert',ARRAY['aa'], ARRAY['dd','ee','ff'])";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            query = "SELECT UNNEST(b_varchar_array) AS A, UNNEST(a_varchar_array) AS B FROM " + tableName + "(b_varchar_array VARCHAR(100) ARRAY)";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("dd",rs.getString("A"));
            assertEquals("aa", rs.getString("B"));
            assertTrue(rs.next());
            assertEquals("ee",rs.getString("A"));
            assertEquals("aa", rs.getString("B"));
            assertTrue(rs.next());
            assertEquals("ff",rs.getString("A"));
            assertEquals("aa", rs.getString("B"));
            assertFalse(rs.next());

        }
    }


    @Test
    public void testUnnestVarcharArrayAndADynamicColumn()throws Exception{
        Properties props = new Properties();
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, getUrl());

        try(Connection conn = DriverManager.getConnection(url, props)){
            String query =
                    "UPSERT INTO " + tableName + "(id, a_varchar_array, b_varchar VARCHAR(100)) "
                            + "VALUES('qwert', ARRAY['dd','ee','ff'],'ab')";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            query = "SELECT UNNEST(a_varchar_array) AS A, b_varchar AS B FROM " + tableName + "(b_varchar VARCHAR(100)) WHERE id = 'qwert'";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("dd",rs.getString("A"));
            assertEquals("ab", rs.getString("B"));
            assertTrue(rs.next());
            assertEquals("ee",rs.getString("A"));
            assertEquals("ab", rs.getString("B"));
            assertTrue(rs.next());
            assertEquals("ff",rs.getString("A"));
            assertEquals("ab", rs.getString("B"));
            assertFalse(rs.next());

        }
    }

    //TODO remove test when post filtering work on where UNNEST()
    @Test
    public void testCountUnnestGroupByUnnestHavingUnnest() throws Exception{
        Properties props = new Properties();
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, getUrl());
        try(Connection conn = DriverManager.getConnection(url, props)){
            String query =
                    "SELECT COUNT(UNNEST(a_varchar_array)) AS A, UNNEST(a_varchar_array) AS B\n"
                            + "FROM " + tableName +"\n"
                            + "GROUP BY B\n"
                            + "HAVING UNNEST(a_varchar_array) = 'XX'";
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(5,rs.getInt("A"));
            assertEquals("XX",rs.getString("B"));
            assertFalse(rs.next());
        }
    }
}
