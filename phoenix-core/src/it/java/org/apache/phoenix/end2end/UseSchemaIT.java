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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.QueryServices;
import org.junit.Test;

public class UseSchemaIT extends BaseHBaseManagedTimeIT {

    @Test
    public void testUseSchema() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE SCHEMA IF NOT EXISTS TEST_SCHEMA";
        conn.createStatement().execute(ddl);
        ddl = "create table test_schema.test(id varchar primary key)";
        conn.createStatement().execute(ddl);
        conn.createStatement().execute("use test_schema");
        String query = "select count(*) from test";
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        try {
            conn.createStatement().execute("use test");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SCHEMA_NOT_FOUND.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testSchemaInJdbcUrl() throws Exception {
        Properties props = new Properties();
        String schema = "TEST_SCHEMA";
        props.setProperty(QueryServices.SCHEMA_ATTRIB, schema);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        String ddl = "CREATE SCHEMA IF NOT EXISTS " + schema;
        conn.createStatement().execute(ddl);
        ddl = "create table IF NOT EXISTS " + schema + ".test(schema_name varchar primary key)";
        conn.createStatement().execute(ddl);
        conn.createStatement().executeUpdate("upsert into " + schema + ".test values('" + schema + "')");
        String query = "select schema_name from test";
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(schema, rs.getString(1));

        schema = "test";
        ddl = "CREATE SCHEMA " + schema;
        conn.createStatement().execute(ddl);
        conn.createStatement().execute("use " + schema);
        ddl = "create table test(schema_name varchar primary key)";
        conn.createStatement().execute(ddl);
        conn.createStatement().executeUpdate("upsert into test values('" + schema + "')");
        rs = conn.createStatement().executeQuery("select schema_name from test");
        assertTrue(rs.next());
        assertEquals(schema, rs.getString(1));
    }
}
