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

import static org.junit.Assert.assertTrue;

@Category(ParallelStatsDisabledTest.class)
public class ShowCreateTableIT extends ParallelStatsDisabledIT {
    @Test
    public void testShowCreateTableBasic() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + "(K VARCHAR NOT NULL PRIMARY KEY, INT INTEGER)";
        conn.createStatement().execute(ddl);

        ResultSet rs = conn.createStatement().executeQuery("SHOW CREATE TABLE " + tableName );
        assertTrue(rs.next());
        assertTrue("Expected: :" + ddl + "\nResult: " + rs.getString(1),
                rs.getString(1).contains(ddl));
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
        assertTrue("Expected: :" + ddl + "\nResult: " + rs.getString(1),
                rs.getString(1).contains(ddl));
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
        assertTrue("Expected: :" + ddl + "\nResult: " + rs.getString(1),
                rs.getString(1).contains(ddl));
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
