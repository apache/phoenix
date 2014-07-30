/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Properties;

import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(HBaseManagedTimeTest.class)
public class PhoenixEncodeDecodeIT extends BaseHBaseManagedTimeIT {
    
    private static String tenantId = "ABC";
    
    @Test
    public void testEncodeDecode() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(
                "CREATE TABLE t(org_id CHAR(3) not null, p_id CHAR(3) not null, date DATE not null, e_id CHAR(3) not null, old_value VARCHAR, new_value VARCHAR " +
                "CONSTRAINT pk PRIMARY KEY (org_id, p_id, date, e_id))");
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO t VALUES (?, ?, ?, ?, ?)");
        Date date = new Date(System.currentTimeMillis());
        stmt.setString(1,  "abc");
        stmt.setString(2,  "def");
        stmt.setDate(3,  date);
        stmt.setString(4,  "eid");
        stmt.setString(5, "old");
        stmt.executeUpdate();
        conn.commit();

        stmt = conn.prepareStatement("SELECT org_id, p_id, date, e_id FROM T");

        Object[] retrievedValues = new Object[4];
        ResultSet rs = stmt.executeQuery();
        rs.next();
        retrievedValues[0] = rs.getString(1);
        retrievedValues[1] = rs.getString(2);
        retrievedValues[2] = rs.getDate(3);
        retrievedValues[3] = rs.getString(4);

        byte[] value = PhoenixRuntime.encodePK(conn, "T", retrievedValues);
        Object[] decodedValues = PhoenixRuntime.decodePK(conn, "T", value);

        assertEquals(Arrays.asList(decodedValues), Arrays.asList(retrievedValues));
    }

    @Test
    public void testEncodeDecodeSalted() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(
                "CREATE TABLE t(org_id CHAR(3) not null, p_id CHAR(3) not null, date DATE not null, e_id CHAR(3) not null, old_value VARCHAR, new_value VARCHAR " +
                "CONSTRAINT pk PRIMARY KEY (org_id, p_id, date, e_id)) SALT_BUCKETS = 2");
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO t VALUES (?, ?, ?, ?, ?)");
        Date date = new Date(System.currentTimeMillis());
        stmt.setString(1,  "abc");
        stmt.setString(2,  "def");
        stmt.setDate(3,  date);
        stmt.setString(4,  "eid");
        stmt.setString(5, "old");
        stmt.executeUpdate();
        conn.commit();

        stmt = conn.prepareStatement("SELECT org_id, p_id, date, e_id FROM T");

        Object[] retrievedValues = new Object[4];
        ResultSet rs = stmt.executeQuery();
        rs.next();
        retrievedValues[0] = rs.getString(1);
        retrievedValues[1] = rs.getString(2);
        retrievedValues[2] = rs.getDate(3);
        retrievedValues[3] = rs.getString(4);

        byte[] value = PhoenixRuntime.encodePK(conn, "T", retrievedValues);
        Object[] decodedValues = PhoenixRuntime.decodePK(conn, "T", value);

        assertEquals(Arrays.asList(decodedValues), Arrays.asList(retrievedValues));
    }

    @Test
    public void testEncodeDecodeMultiTenant() throws Exception {
        Connection globalConn = DriverManager.getConnection(getUrl());
        try {
            globalConn.createStatement().execute(
                    "CREATE TABLE T(tenant_id CHAR(3) not null, p_id CHAR(3) not null, date DATE not null, e_id CHAR(3) not null, old_value VARCHAR, new_value VARCHAR " +
                    "CONSTRAINT pk PRIMARY KEY (tenant_id, p_id, date, e_id)) MULTI_TENANT = true");
        } finally {
            globalConn.close();
        }

        Connection tenantConn = getTenantSpecificConnection();

        //create tenant-specific view. 
        tenantConn.createStatement().execute("CREATE VIEW TENANT_TABLE AS SELECT * FROM T");
        
        PreparedStatement stmt = tenantConn.prepareStatement("UPSERT INTO TENANT_TABLE (p_id, date, e_id) VALUES (?, ?, ?)");
        Date date = new Date(System.currentTimeMillis());
        stmt.setString(1,  "def");
        stmt.setDate(2,  date);
        stmt.setString(3,  "eid");
        stmt.executeUpdate();
        tenantConn.commit();

        stmt = tenantConn.prepareStatement("SELECT p_id, date, e_id FROM TENANT_TABLE");

        Object[] retrievedValues = new Object[3];
        ResultSet rs = stmt.executeQuery();
        rs.next();
        retrievedValues[0] = rs.getString(1);
        retrievedValues[1] = rs.getDate(2);
        retrievedValues[2] = rs.getString(3);

        byte[] value = PhoenixRuntime.encodePK(tenantConn, "TENANT_TABLE", retrievedValues);
        Object[] decodedValues = PhoenixRuntime.decodePK(tenantConn, "TENANT_TABLE", value);

        assertEquals(Arrays.asList(decodedValues), Arrays.asList(retrievedValues));
    }

    @Test
    public void testEncodeDecodeSaltedMultiTenant() throws Exception {
        Connection globalConn = DriverManager.getConnection(getUrl());
        try {
            globalConn.createStatement().execute(
                    "CREATE TABLE T(tenant_id CHAR(3) not null, p_id CHAR(3) not null, date DATE not null, e_id CHAR(3) not null, old_value VARCHAR, new_value VARCHAR " +
                    "CONSTRAINT pk PRIMARY KEY (tenant_id, p_id, date, e_id)) MULTI_TENANT = true, SALT_BUCKETS = 2");
        } finally {
            globalConn.close();
        }

        Connection tenantConn = getTenantSpecificConnection();

        //create tenant-specific view. 
        tenantConn.createStatement().execute("CREATE VIEW TENANT_TABLE AS SELECT * FROM T");
        
        PreparedStatement stmt = tenantConn.prepareStatement("UPSERT INTO TENANT_TABLE (p_id, date, e_id) VALUES (?, ?, ?)");
        Date date = new Date(System.currentTimeMillis());
        stmt.setString(1,  "def");
        stmt.setDate(2,  date);
        stmt.setString(3,  "eid");
        stmt.executeUpdate();
        tenantConn.commit();

        stmt = tenantConn.prepareStatement("SELECT p_id, date, e_id FROM TENANT_TABLE");

        Object[] retrievedValues = new Object[3];
        ResultSet rs = stmt.executeQuery();
        rs.next();
        retrievedValues[0] = rs.getString(1);
        retrievedValues[1] = rs.getDate(2);
        retrievedValues[2] = rs.getString(3);

        byte[] value = PhoenixRuntime.encodePK(tenantConn, "TENANT_TABLE", retrievedValues);
        Object[] decodedValues = PhoenixRuntime.decodePK(tenantConn, "TENANT_TABLE", value);

        assertEquals(Arrays.asList(decodedValues), Arrays.asList(retrievedValues));
    }
    
    @Test
    public void testEncodeDecodePaddingPks() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(
                "CREATE TABLE T(pk1 CHAR(15) not null, pk2 CHAR(15) not null, v1 DATE " +
                "CONSTRAINT pk PRIMARY KEY (pk1, pk2))");
        
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO T (pk1, pk2, v1) VALUES (?, ?, ?)");
        stmt.setString(1,  "def");
        stmt.setString(2,  "eid");
        stmt.setDate(3, new Date(100));
        stmt.executeUpdate();
        conn.commit();

        stmt = conn.prepareStatement("SELECT pk1, pk2 FROM T");

        Object[] retrievedValues = new Object[2];
        ResultSet rs = stmt.executeQuery();
        rs.next();
        retrievedValues[0] = rs.getString(1);
        retrievedValues[1] = rs.getString(2);
        
        byte[] value = PhoenixRuntime.encodePK(conn, "T", retrievedValues);
        Object[] decodedValues = PhoenixRuntime.decodePK(conn, "T", value);

        assertEquals(Arrays.asList(decodedValues), Arrays.asList(retrievedValues));
    }

    private static Connection getTenantSpecificConnection() throws Exception {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getUrl(), props);
    }

}
