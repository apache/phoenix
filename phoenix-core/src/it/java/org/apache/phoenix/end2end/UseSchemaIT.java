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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

public class UseSchemaIT extends ParallelStatsDisabledIT {

    @Test
    public void testUseSchemaCaseInsensitive() throws Exception {
        String schemaName = generateUniqueName();
        testUseSchema(schemaName);
    }

    @Test
    public void testUseSchemaCaseSensitive() throws Exception {
        String schemaName = generateUniqueName();
        testUseSchema("\"" + schemaName + "\"");
    }

    public void testUseSchema(String schema) throws Exception {
        Properties props = new Properties();
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE SCHEMA IF NOT EXISTS "+schema;
        conn.createStatement().execute(ddl);
        String testTable = generateUniqueName();
        ddl = "create table "+schema+"." + testTable + "(id varchar primary key)";
        conn.createStatement().execute(ddl);
        conn.createStatement().execute("use "+schema);
        String query = "select count(*) from " + testTable;
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        try {
            conn.createStatement().execute("use " + testTable);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SCHEMA_NOT_FOUND.getErrorCode(), e.getErrorCode());
        }
        conn.createStatement().execute("use default");
        ddl = "create table IF NOT EXISTS " + testTable + "(schema_name varchar primary key)";
        conn.createStatement().execute(ddl);
        conn.createStatement().executeUpdate("upsert into " + testTable + " values('"+SchemaUtil.SCHEMA_FOR_DEFAULT_NAMESPACE+"')");
        conn.commit();
        rs = conn.createStatement().executeQuery("select schema_name from " + testTable);
        assertTrue(rs.next());
        assertEquals(SchemaUtil.SCHEMA_FOR_DEFAULT_NAMESPACE, rs.getString(1));
        conn.close();
    }

    @Test
    public void testSchemaInJdbcUrl() throws Exception {
        Properties props = new Properties();
        String schema = generateUniqueName();
        props.setProperty(QueryServices.SCHEMA_ATTRIB, schema);
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        String ddl = "CREATE SCHEMA IF NOT EXISTS " + schema;
        conn.createStatement().execute(ddl);
        String testTable = generateUniqueName();
        ddl = "create table IF NOT EXISTS " + schema + "." + testTable + " (schema_name varchar primary key)";
        conn.createStatement().execute(ddl);
        conn.createStatement().executeUpdate("upsert into " + schema + "." + testTable + " values('" + schema + "')");
        String query = "select schema_name from " + testTable;
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(schema, rs.getString(1));

        schema = generateUniqueName();
        ddl = "CREATE SCHEMA " + schema;
        conn.createStatement().execute(ddl);
        conn.createStatement().execute("use " + schema);
        ddl = "create table " + testTable + "(schema_name varchar primary key)";
        conn.createStatement().execute(ddl);
        conn.createStatement().executeUpdate("upsert into " + testTable + " values('" + schema + "')");
        rs = conn.createStatement().executeQuery("select schema_name from " + testTable );
        assertTrue(rs.next());
        assertEquals(schema, rs.getString(1));
        conn.createStatement().execute("DROP TABLE " + testTable);
        conn.close();
    }
    
    @Test
    public void testSequences() throws Exception {
        Properties props = new Properties();
        String schema = generateUniqueName();
        props.setProperty(QueryServices.SCHEMA_ATTRIB, schema);
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        String ddl = "CREATE SCHEMA IF NOT EXISTS " + schema;
        conn.createStatement().execute(ddl);
        String sequenceName = generateUniqueName();
        ddl = "create SEQUENCE "+schema + "." + sequenceName + " START WITH 100 INCREMENT BY 2 CACHE 10";
        conn.createStatement().execute(ddl);
        String query = "SELECT NEXT VALUE FOR "+schema + "." + sequenceName;
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("100", rs.getString(1));
        conn.createStatement().execute("DROP Sequence " + schema + "." + sequenceName);
        
        schema = generateUniqueName();
        sequenceName = generateUniqueName();
        ddl = "CREATE SCHEMA " + schema;
        conn.createStatement().execute(ddl);
        conn.createStatement().execute("use " + schema);
        ddl = "create SEQUENCE "+ sequenceName + " START WITH 100 INCREMENT BY 2 CACHE 10";
        conn.createStatement().execute(ddl);
        query = "SELECT NEXT VALUE FOR "+sequenceName;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("100", rs.getString(1));
        query = "SELECT CURRENT VALUE FOR "+sequenceName;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("100", rs.getString(1));
        conn.createStatement().execute("DROP Sequence " + sequenceName);
        conn.close();
    }

    @Test
    public void testMappedView() throws Exception {
        Properties props = new Properties();
        String schema = generateUniqueName();
        String tableName = generateUniqueName();
        String fullTablename = schema + QueryConstants.NAME_SEPARATOR + tableName;
        props.setProperty(QueryServices.SCHEMA_ATTRIB, schema);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        admin.createNamespace(NamespaceDescriptor.create(schema).build());
        admin.createTable(new HTableDescriptor(fullTablename)
                .addFamily(new HColumnDescriptor(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES)));
        Put put = new Put(PVarchar.INSTANCE.toBytes(fullTablename));
        put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES,
                QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
        HTable phoenixSchematable = new HTable(admin.getConfiguration(), fullTablename);
        phoenixSchematable.put(put);
        phoenixSchematable.close();
        conn.createStatement().execute("CREATE VIEW " + tableName + " (tablename VARCHAR PRIMARY KEY)");
        ResultSet rs = conn.createStatement().executeQuery("select tablename from " + tableName);
        assertTrue(rs.next());
        assertEquals(fullTablename, rs.getString(1));
        admin.close();
        conn.close();
    }
}
