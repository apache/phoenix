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

import static org.apache.phoenix.util.TestUtil.A_VALUE;
import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.junit.Test;

public class UpsertSelectAutoCommitIT extends BaseHBaseManagedTimeIT {

    public UpsertSelectAutoCommitIT() {
    }

    @Test
    public void testAutoCommitUpsertSelect() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        conn.createStatement().execute("CREATE TABLE atable (ORGANIZATION_ID CHAR(15) NOT NULL, ENTITY_ID CHAR(15) NOT NULL, A_STRING VARCHAR\n" +
        "CONSTRAINT pk PRIMARY KEY (organization_id, entity_id))");
        
        String tenantId = getOrganizationId();
       // Insert all rows at ts
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " +
                "ATABLE(" +
                "    ORGANIZATION_ID, " +
                "    ENTITY_ID, " +
                "    A_STRING " +
                "    )" +
                "VALUES (?, ?, ?)");
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW1);
        stmt.setString(3, A_VALUE);
        stmt.execute();
        
        String query = "SELECT entity_id, a_string FROM ATABLE";
        PreparedStatement statement = conn.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        
        assertTrue(rs.next());
        assertEquals(ROW1, rs.getString(1));
        assertEquals(A_VALUE, rs.getString(2));
        assertFalse(rs.next());
        
        conn.createStatement().execute("CREATE TABLE atable2 (ORGANIZATION_ID CHAR(15) NOT NULL, ENTITY_ID CHAR(15) NOT NULL, A_STRING VARCHAR\n" +
        "CONSTRAINT pk PRIMARY KEY (organization_id, entity_id DESC))");
        
        conn.createStatement().execute("UPSERT INTO atable2 SELECT * FROM ATABLE");
        query = "SELECT entity_id, a_string FROM ATABLE2";
        statement = conn.prepareStatement(query);
        rs = statement.executeQuery();
        
        assertTrue(rs.next());
        assertEquals(ROW1, rs.getString(1));
        assertEquals(A_VALUE, rs.getString(2));
        assertFalse(rs.next());
        
    }

    @Test
    public void testDynamicUpsertSelect() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String cursorDDL = " CREATE TABLE IF NOT EXISTS CURSOR (ORGANIZATION_ID VARCHAR(15) NOT NULL, \n"
                + "QUERY_ID VARCHAR(15) NOT NULL, \n"
                + "CURSOR_ORDER UNSIGNED_LONG NOT NULL, \n"
                + "CONSTRAINT API_HBASE_CURSOR_STORAGE_PK PRIMARY KEY (ORGANIZATION_ID, QUERY_ID, CURSOR_ORDER))\n"
                + "SALT_BUCKETS = 4";
        conn.createStatement().execute(cursorDDL);
        
        String dataTableDDL = "CREATE TABLE IF NOT EXISTS PLINYTEST" +
                "(" +
                "ORGANIZATION_ID CHAR(15) NOT NULL, " +
                "PLINY_ID CHAR(15) NOT NULL, " +
                "CREATED_DATE DATE NOT NULL, " + 
                "TEXT VARCHAR, " +
                "CONSTRAINT PK PRIMARY KEY " +
                "(" +
                "ORGANIZATION_ID, " +
                "PLINY_ID, "  +
                "CREATED_DATE" +
                ")" +
                ")";
        
        conn.createStatement().execute(dataTableDDL);
        PreparedStatement stmt = null;
        String upsert = "UPSERT INTO PLINYTEST VALUES (?, ?, ?, ?)";
        stmt = conn.prepareStatement(upsert);
        stmt.setString(1, getOrganizationId());
        stmt.setString(2, "aaaaaaaaaaaaaaa");
        stmt.setDate(3, new Date(System.currentTimeMillis()));
        stmt.setString(4, "text");
        stmt.executeUpdate();
        conn.commit();
        
        String upsertSelect = "UPSERT INTO CURSOR (ORGANIZATION_ID, QUERY_ID, CURSOR_ORDER, PLINY_ID CHAR(15),CREATED_DATE DATE) SELECT ?, ?, ?, PLINY_ID, CREATED_DATE FROM PLINYTEST WHERE ORGANIZATION_ID = ?";
        stmt = conn.prepareStatement(upsertSelect);
        String orgId = getOrganizationId();
        stmt.setString(1, orgId);
        stmt.setString(2, "queryqueryquery");

        stmt.setInt(3, 1);
        stmt.setString(4, orgId);
        stmt.executeUpdate();
        conn.commit();
    }
    
}
