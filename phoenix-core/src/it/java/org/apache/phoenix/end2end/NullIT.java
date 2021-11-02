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

import static org.apache.phoenix.util.TestUtil.C_VALUE;
import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.ROW2;
import static org.apache.phoenix.util.TestUtil.ROW3;
import static org.apache.phoenix.util.TestUtil.ROW4;
import static org.apache.phoenix.util.TestUtil.ROW5;
import static org.apache.phoenix.util.TestUtil.ROW6;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.ROW8;
import static org.apache.phoenix.util.TestUtil.ROW9;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

@Category(ParallelStatsDisabledTest.class)
public class NullIT extends BaseQueryIT {
    
    @Parameters(name="NullIT_{index},columnEncoded={1}")
    public static synchronized Collection<Object> data() {
        return BaseQueryIT.allIndexesWithEncoded();
    }
    
    public NullIT(String indexDDL, boolean columnEncoded, boolean keepDeletedCells) {
        super(indexDDL, columnEncoded, keepDeletedCells);
    }
    
    private void testNoStringValue(String value) throws Exception {
        String url = getUrl();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        // Insert all rows at ts
        PreparedStatement stmt = upsertConn.prepareStatement(
                "upsert into " + tableName + " VALUES (?, ?, ?)"); // without specifying columns
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW5);
        stmt.setString(3, value);
        stmt.execute(); // should commit too
        upsertConn.close();
        
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        TestUtil.analyzeTable(conn1, tableName);
        conn1.close();
        
        String query = "SELECT a_string, b_string FROM " + tableName + " WHERE organization_id=? and a_integer = 5";
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(null, rs.getString(1));
            assertTrue(rs.wasNull());
            assertEquals(C_VALUE, rs.getString("B_string"));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testNullStringValue() throws Exception {
        testNoStringValue(null);
    }
    
    @Test
    public void testEmptyStringValue() throws Exception {
        testNoStringValue("");
    }
    
    @Test
    public void testIsNull() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " WHERE X_DECIMAL is null";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW1);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW3);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW4);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW5);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW6);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testIsNotNull() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " WHERE X_DECIMAL is not null";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW7);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW8);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW9);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    // PHOENIX-6583
    @Test
    public void testBinaryNullAssignment() throws SQLException {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);

        ResultSet rs;

        try (Statement stmt = conn.createStatement()) {

            String binTestTable=generateUniqueName();

            stmt.execute("create table "+binTestTable+" (id integer not null, text varchar(255), testbin binary(16), CONSTRAINT pk primary key (id))");
            conn.commit();

            String queryIsNull = "select id, text , testbin from "+binTestTable+" where testbin is null";


            // Let's see if without providing it, it is stored as null
            stmt.execute("upsert into "+binTestTable+"  (id,text) values (1,'anytext')");
            conn.commit();
            rs= stmt.executeQuery(queryIsNull);
            assertTrue(rs.next());
            rs.close();

            // Let's see if providing it, but it is set as null,  it is also stored as null
            stmt.execute("upsert into "+binTestTable+"  (id,text,testbin) values (1,'anytext',null)");
            conn.commit();
            rs = stmt.executeQuery(queryIsNull);
            assertTrue(rs.next());
            rs.close();

            //Now let's set a value. Now It should be NOT null
            stmt.execute("upsert into "+binTestTable+"  (id,text,testbin) values (1,'anytext','a')");
            conn.commit();
            rs = stmt.executeQuery(queryIsNull);
            assertTrue(false == rs.next());
            rs.close();

            //Right now it has a value.... let's see if we can set it again a null value
            stmt.execute("upsert into "+binTestTable+"  (id,text,testbin) values (1,'anytext',null)");
            conn.commit();
            rs = stmt.executeQuery(queryIsNull);
            assertTrue(rs.next());
            rs.close();

            stmt.execute("DROP TABLE "+binTestTable+" ");
            conn.commit();

            rs.close();
        }
    }
}
