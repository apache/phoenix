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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.ColumnAlreadyExistsException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.mockito.Mockito;

public class AppendOnlySchemaIT extends ParallelStatsDisabledIT {

    
    private void testTableWithSameSchema(boolean notExists, boolean sameClient) throws Exception {

        // use a spyed ConnectionQueryServices so we can verify calls to getTable
        ConnectionQueryServices connectionQueryServices =
                Mockito.spy(driver.getConnectionQueryServices(getUrl(),
                    PropertiesUtil.deepCopy(TEST_PROPERTIES)));
        Properties props = new Properties();
        props.putAll(PhoenixEmbeddedDriver.DEFFAULT_PROPS.asMap());

        try (Connection conn1 = connectionQueryServices.connect(getUrl(), props);
                Connection conn2 = sameClient ? conn1 : connectionQueryServices.connect(getUrl(), props)) {

            String metricTableName = generateUniqueName();
            String viewName = generateUniqueName();
            String metricIdSeqTableName = generateUniqueName();
            // create sequence for auto partition
            conn1.createStatement().execute("CREATE SEQUENCE " + metricIdSeqTableName + " CACHE 1");
            // create base table
            conn1.createStatement().execute("CREATE TABLE "+ metricTableName + "(metricId INTEGER NOT NULL, metricVal DOUBLE, CONSTRAINT PK PRIMARY KEY(metricId))"
                    + " APPEND_ONLY_SCHEMA = true, UPDATE_CACHE_FREQUENCY=1, AUTO_PARTITION_SEQ=" + metricIdSeqTableName);
            // create view
            String ddl =
                    "CREATE VIEW " + (notExists ? "IF NOT EXISTS " : "")
                            + viewName + " ( hostName varchar NOT NULL, tagName varChar"
                            + " CONSTRAINT HOSTNAME_PK PRIMARY KEY (hostName))"
                            + " AS SELECT * FROM " + metricTableName
                            + " UPDATE_CACHE_FREQUENCY=300000";
            conn1.createStatement().execute(ddl);
            conn1.createStatement().execute("UPSERT INTO " + viewName + "(hostName, metricVal) VALUES('host1', 1.0)");
            conn1.commit();
            reset(connectionQueryServices);

            // execute same create ddl
            try {
                conn2.createStatement().execute(ddl);
                if (!notExists) {
                    fail("Create Table should fail");
                }
            }
            catch (TableAlreadyExistsException e) {
                if (notExists) {
                    fail("Create Table should not fail");
                }
            }
            
            // verify getTable rpcs
            verify(connectionQueryServices, sameClient ? never() : times(1)).getTable((PName)isNull(), eq(new byte[0]), eq(Bytes.toBytes(viewName)), anyLong(), anyLong());
            
            // verify no create table rpcs
            verify(connectionQueryServices, never()).createTable(anyListOf(Mutation.class),
                any(byte[].class), any(PTableType.class), anyMap(), anyList(), any(byte[][].class),
                eq(false), eq(false));
            reset(connectionQueryServices);
            
            // execute alter table ddl that adds the same column
            ddl = "ALTER VIEW " + viewName + " ADD " + (notExists ? "IF NOT EXISTS" : "") + " tagName varchar";
            try {
                conn2.createStatement().execute(ddl);
                if (!notExists) {
                    fail("Alter Table should fail");
                }
            }
            catch (ColumnAlreadyExistsException e) {
                if (notExists) {
                    fail("Alter Table should not fail");
                }
            }
            
            // if not verify exists is true one call to add column table with empty mutation list (which does not make a rpc) 
            // else verify no add column calls
            verify(connectionQueryServices, notExists ? times(1) : never() ).addColumn(eq(Collections.<Mutation>emptyList()), any(PTable.class), anyMap(), anySetOf(String.class), anyListOf(PColumn.class));

            // upsert one row
            conn2.createStatement().execute("UPSERT INTO " + viewName + "(hostName, metricVal) VALUES('host2', 2.0)");
            conn2.commit();
            // verify data in base table
            ResultSet rs = conn2.createStatement().executeQuery("SELECT * from " + metricTableName);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(1.0, rs.getDouble(2), 1e-6);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(2.0, rs.getDouble(2), 1e-6);
            assertFalse(rs.next());
            // verify data in view
            rs = conn2.createStatement().executeQuery("SELECT * from " + viewName);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(1.0, rs.getDouble(2), 1e-6);
            assertEquals("host1", rs.getString(3));
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(2.0, rs.getDouble(2), 1e-6);
            assertEquals("host2", rs.getString(3));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSameSchemaWithNotExistsSameClient() throws Exception {
        testTableWithSameSchema(true, true);
    }
    
    @Test
    public void testSameSchemaWithNotExistsDifferentClient() throws Exception {
        testTableWithSameSchema(true, false);
    }
    
    @Test
    public void testSameSchemaSameClient() throws Exception {
        testTableWithSameSchema(false, true);
    }
    
    @Test
    public void testSameSchemaDifferentClient() throws Exception {
        testTableWithSameSchema(false, false);
    }

    private void testAddColumns(boolean sameClient) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn1 = DriverManager.getConnection(getUrl(), props);
                Connection conn2 = sameClient ? conn1 : DriverManager.getConnection(getUrl(), props)) {

            String metricTableName = generateUniqueName();
            String viewName = generateUniqueName();
            String metricIdSeqTableName = generateUniqueName();

            // create sequence for auto partition
            conn1.createStatement().execute("CREATE SEQUENCE " + metricIdSeqTableName + " CACHE 1");
            // create base table
            conn1.createStatement().execute("CREATE TABLE " + metricTableName + " (metricId INTEGER NOT NULL, metricVal1 DOUBLE, CONSTRAINT PK PRIMARY KEY(metricId))"
                    + " APPEND_ONLY_SCHEMA = true, UPDATE_CACHE_FREQUENCY=1, AUTO_PARTITION_SEQ=" + metricIdSeqTableName);
            // create view
            String ddl =
                    "CREATE VIEW IF NOT EXISTS "
                            + viewName + "( hostName varchar NOT NULL,"
                            + " CONSTRAINT HOSTNAME_PK PRIMARY KEY (hostName))"
                            + " AS SELECT * FROM " + metricTableName
                            + " UPDATE_CACHE_FREQUENCY=300000";
            conn1.createStatement().execute(ddl);
            
            conn1.createStatement().execute("UPSERT INTO " + viewName + "(hostName, metricVal1) VALUES('host1', 1.0)");
            conn1.commit();

            // execute ddl that creates that same view with an additional pk column and regular column
            // and also changes the order of the pk columns (which is not respected since we only 
            // allow appending columns)
            ddl =
                    "CREATE VIEW IF NOT EXISTS "
                            + viewName + "( instanceName varchar, hostName varchar, metricVal2 double, metricVal1 double"
                            + " CONSTRAINT HOSTNAME_PK PRIMARY KEY (instancename, hostName))"
                            + " AS SELECT * FROM " + metricTableName
                            + " UPDATE_CACHE_FREQUENCY=300000";
            conn2.createStatement().execute(ddl);

            conn2.createStatement().execute(
                "UPSERT INTO " + viewName + "(hostName, instanceName, metricVal1, metricval2) VALUES('host2', 'instance2', 21.0, 22.0)");
            conn2.commit();
            
            conn1.createStatement().execute("UPSERT INTO " + viewName + "(hostName, metricVal1) VALUES('host3', 3.0)");
            conn1.commit();
            
            // verify data exists
            ResultSet rs = conn2.createStatement().executeQuery("SELECT * from " + viewName);
            
            // verify the two columns were added correctly
            PTable table =
                    conn2.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, viewName));
            List<PColumn> pkColumns = table.getPKColumns();
            assertEquals(3,table.getPKColumns().size());
            // even though the second create view statement changed the order of the pk, the original order is maintained
            PColumn metricId = pkColumns.get(0);
            assertEquals("METRICID", metricId.getName().getString());
            assertFalse(metricId.isNullable());
            PColumn hostName = pkColumns.get(1);
            assertEquals("HOSTNAME", hostName.getName().getString());
            // hostname name is not nullable even though the second create statement changed it to nullable
            // since we only allow appending columns
            assertFalse(hostName.isNullable());
            PColumn instanceName = pkColumns.get(2);
            assertEquals("INSTANCENAME", instanceName.getName().getString());
            assertTrue(instanceName.isNullable());
            List<PColumn> columns = table.getColumns();
            assertEquals("METRICID", columns.get(0).getName().getString());
            assertEquals("METRICVAL1", columns.get(1).getName().getString());
            assertEquals("HOSTNAME", columns.get(2).getName().getString());
            assertEquals("INSTANCENAME", columns.get(3).getName().getString());
            assertEquals("METRICVAL2", columns.get(4).getName().getString());
            
            // verify the data
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(1.0, rs.getDouble(2), 1e-6);
            assertEquals("host1", rs.getString(3));
            assertEquals(null, rs.getString(4));
            assertEquals(0.0, rs.getDouble(5), 1e-6);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(21.0, rs.getDouble(2), 1e-6);
            assertEquals("host2", rs.getString(3));
            assertEquals("instance2", rs.getString(4));
            assertEquals(22.0, rs.getDouble(5), 1e-6);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(3.0, rs.getDouble(2), 1e-6);
            assertEquals("host3", rs.getString(3));
            assertEquals(null, rs.getString(4));
            assertEquals(0.0, rs.getDouble(5), 1e-6);
            assertFalse(rs.next());
        }
    }
    
    @Test
    public void testAddColumnsSameClient() throws Exception {
        testAddColumns(true);
    }
    
    @Test
    public void testTableAddColumnsDifferentClient() throws Exception {
        testAddColumns(false);
    }

    @Test
    public void testValidateAttributes() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String tableName = generateUniqueName();
            String viewName = generateUniqueName();
            try {
                conn.createStatement().execute(
                    "create table IF NOT EXISTS " + tableName + " ( id char(1) NOT NULL,"
                            + " col1 integer NOT NULL,"
                            + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1))"
                            + " APPEND_ONLY_SCHEMA = true");
                fail("UPDATE_CACHE_FREQUENCY attribute must not be set to ALWAYS if APPEND_ONLY_SCHEMA is true");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.UPDATE_CACHE_FREQUENCY_INVALID.getErrorCode(),
                    e.getErrorCode());
            }
            
            conn.createStatement().execute(
                "create table IF NOT EXISTS " + tableName + " ( id char(1) NOT NULL,"
                        + " col1 integer NOT NULL"
                        + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1))"
                        + " APPEND_ONLY_SCHEMA = true, UPDATE_CACHE_FREQUENCY=1000");
            conn.createStatement().execute(
                "create view IF NOT EXISTS " + viewName + " (val1 integer) AS SELECT * FROM " + tableName);
            PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
            PTable view = pconn.getTable(new PTableKey(pconn.getTenantId(), viewName));
            assertEquals(true, view.isAppendOnlySchema());
            assertEquals(1000, view.getUpdateCacheFrequency());
        }
    }
    
    @Test
    public void testUpsertRowToDeletedTable() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn1 = DriverManager.getConnection(getUrl(), props);
                Connection conn2 = DriverManager.getConnection(getUrl(), props)) {
            String metricTableName = generateUniqueName();
            String viewName = generateUniqueName();
            String metricIdSeqTableName = generateUniqueName();
            // create sequence for auto partition
            conn1.createStatement().execute("CREATE SEQUENCE " + metricIdSeqTableName + "  CACHE 1");
            // create base table
            conn1.createStatement().execute("CREATE TABLE " + metricTableName + " (metricId INTEGER NOT NULL, metricVal DOUBLE, CONSTRAINT PK PRIMARY KEY(metricId))"
                    + " APPEND_ONLY_SCHEMA = true, UPDATE_CACHE_FREQUENCY=1, AUTO_PARTITION_SEQ=" + metricIdSeqTableName);
            // create view
            String ddl =
                    "CREATE VIEW IF NOT EXISTS "
                            + viewName + "( hostName varchar NOT NULL,"
                            + " CONSTRAINT HOSTNAME_PK PRIMARY KEY (hostName))"
                            + " AS SELECT * FROM " + metricTableName
                            + " APPEND_ONLY_SCHEMA = true, UPDATE_CACHE_FREQUENCY=300000";
            conn1.createStatement().execute(ddl);
            
            // drop the table using a different connection
            conn2.createStatement().execute("DROP VIEW " + viewName);
            
            // upsert one row
            conn1.createStatement().execute("UPSERT INTO " + viewName + "(hostName, metricVal) VALUES('host1', 1.0)");
            // upsert doesn't fail since base table still exists
            conn1.commit();
        }
    }

}
