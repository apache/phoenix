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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.schema.SequenceNotFoundException;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;

@RunWith(Parameterized.class)
public class TenantIdTypeIT extends ParallelStatsDisabledIT {

    private Connection regularConnection(String url) throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
        return DriverManager.getConnection(url, props);
    }

    private Connection tenantConnection(String url) throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
        String tenantIdProperty = this.tenantId.replaceAll("\'", "");
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantIdProperty);
        return DriverManager.getConnection(url, props);
    }

    private Connection inconvertibleConnection(String url) throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
        String tenantIdProperty = "ABigOlString";
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantIdProperty);
        return DriverManager.getConnection(url, props);
    }

    private final String ddl;
    private final String dataType;
    private final String tenantId;
    private final String otherTenantId;
    private final String table;
    private final String view;
    private final String sequence;

    public TenantIdTypeIT(String dataType, String tenantId, String otherTenantId) {
        this.dataType = dataType;
        this.tenantId = tenantId;
        this.otherTenantId = otherTenantId;
        String tbl = generateUniqueName();
        if(tbl.contains("(")){
            tbl = tbl.substring(0, tbl.indexOf("("));
        }
        this.table = tbl;
        this.view = tbl + "view";
        this.sequence = tbl + "sequence";
        this.ddl = "create table " + table + " (" + "tid "+ dataType + " NOT NULL," + "id INTEGER NOT NULL, \n"
                + "val VARCHAR " + "CONSTRAINT pk PRIMARY KEY(tid, id)) \n"
                + "MULTI_TENANT=true";
    }

    @Parameters( name = "TenantIdTypeIT_datatype={0}" ) // name is used by failsafe as file name in reports
    public static Collection<Object[]> data() {
        List<Object[]> testCases = Lists.newArrayList();
        testCases.add(new Object[] { "INTEGER", "2147483647", "2147483646" });
        testCases.add(new Object[] { "UNSIGNED_INT", "2147483647", "2147483646" });
        testCases.add(new Object[] { "BIGINT", "9223372036854775807", "9223372036854775806" });
        testCases.add(new Object[] { "UNSIGNED_LONG", "9223372036854775807", "9223372036854775806" });
        testCases.add(new Object[] { "TINYINT", "127", "126" });
        testCases.add(new Object[] { "UNSIGNED_TINYINT", "85", "84" });
        testCases.add(new Object[] { "SMALLINT", "32767", "32766" });
        testCases.add(new Object[] { "UNSIGNED_SMALLINT", "32767", "32766" });
        testCases.add(new Object[] { "FLOAT", "3.4028234", "3.4028232" });
        testCases.add(new Object[] { "UNSIGNED_FLOAT", "3.4028234", "3.4028232" });
        testCases.add(new Object[] { "DOUBLE", "1.7976931348623157", "1.7976931348623156" });
        testCases.add(new Object[] { "UNSIGNED_DOUBLE", "1.7976931348623157", "1.7976931348623156" });
        testCases.add(new Object[] { "DECIMAL", "3.402823466", "3.402823465" });
        testCases.add(new Object[] { "VARCHAR", "\'NameOfTenant\'", "\'Nemesis\'" });
        testCases.add(new Object[] { "CHAR(10)", "\'1234567890\'", "\'Nemesis\'" });

        return testCases;
    }

    @Test
    public void testMultiTenantTables() throws Exception {
        //Verify we can create the table
        try (Connection conn = regularConnection(getUrl())) {
            conn.setAutoCommit(true);
            conn.createStatement().execute(ddl);

            try {
                conn.createStatement().execute(ddl);
                fail("Table with " + dataType + " tenantId not created correctly");
            } catch (TableAlreadyExistsException e) {
                // expected
            }
        }

        //Insert test data
        try (Connection conn = regularConnection(getUrl())) {
            conn.setAutoCommit(true);
            String query = "upsert into " + table +
                    " values (" + tenantId + ", 1 , 'valid')";

            conn.createStatement().execute("upsert into " + table +
                    " values (" + tenantId + ", 1 , 'valid')");
            conn.createStatement().execute("upsert into " + table +
                    " values (" + otherTenantId + ", 2 , 'invalid')");
        }

        //Make sure access is properly restricted and add some tenant-specific schema
        try (Connection conn = tenantConnection(getUrl())) {
            conn.setAutoCommit(true);
            ResultSet rs = conn.createStatement().executeQuery("select * from " + table);
            assertTrue("Expected 1 row in result set", rs.next());
            assertEquals("valid", rs.getString(2));
            assertFalse("Expected 1 row in result set", rs.next());

            try {
                conn.createStatement()
                        .executeQuery("select * from " + table + " where tenantId = 2");
                fail("TenantId column not hidden on multi-tenant connection");
            } catch (SQLException ex) {
                assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(), ex.getErrorCode());
            }

            conn.createStatement().execute("create view " + view +
                    " as select * from " + table);

            conn.createStatement().execute("create sequence " + sequence + " start with 100");
        }

        //Try inserting data to the view
        try (Connection conn = tenantConnection(getUrl())) {
            conn.setAutoCommit(true);
            conn.createStatement().execute("upsert into " + view +
                    " values ( next value for " + sequence + ", 'valid')");
        }

        //Try reading data from the view
        try (Connection conn = tenantConnection(getUrl())) {
            ResultSet rs = conn.createStatement().executeQuery("select * from " + view);
            assertTrue("Expected 2 rows in result set", rs.next());
            assertEquals("valid", rs.getString(2));
            assertTrue("Expected 2 rows in result set", rs.next());
            assertEquals("valid", rs.getString(2));
            assertFalse("Expected 2 rows in result set", rs.next());
        }

        //Make sure the tenant-specific schema is specific to that tenant
        try (Connection conn = regularConnection(getUrl())) {
            try {
                conn.createStatement().execute("upsert into " + table +
                        " values (" + tenantId + ", next value for " + sequence + ", 'valid')");
                fail();
            } catch (SequenceNotFoundException ex) {}

            try {
                ResultSet rs = conn.createStatement().executeQuery("select * from " + view);
                fail();
            } catch (SQLException ex) {
                assertEquals(SQLExceptionCode.TABLE_UNDEFINED.getErrorCode(), ex.getErrorCode());
            }

        }

        if(dataType != "VARCHAR" && dataType != "CHAR(10)") {
            //Try setting up an invalid tenant-specific view
            try (Connection conn = inconvertibleConnection(getUrl())) {
                conn.setAutoCommit(true);
                conn.createStatement().execute("create view " + view +
                        " as select * from " + table);
            }

            //Try inserting data to the invalid tenant-specific view
            try (Connection conn = inconvertibleConnection(getUrl())) {
                conn.setAutoCommit(true);
                try {
                    conn.createStatement().execute("upsert into " + view +
                            " values ( 3 , 'invalid')");
                    fail();
                } catch (SQLException ex) {
                    assertEquals(SQLExceptionCode.TENANTID_IS_OF_WRONG_TYPE.getErrorCode(), ex.getErrorCode());
                }
            }

            //Try reading data from the invalid tenant-specific view
            try (Connection conn = inconvertibleConnection(getUrl())) {
                try {
                    ResultSet rs = conn.createStatement().executeQuery("select * from " + view);
                    fail();
                } catch (SQLException ex) {
                    assertEquals(SQLExceptionCode.TENANTID_IS_OF_WRONG_TYPE.getErrorCode(), ex.getErrorCode());
                }
            }
        }

    }
}
