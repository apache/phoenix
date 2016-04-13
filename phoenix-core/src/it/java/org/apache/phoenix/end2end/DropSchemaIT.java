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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.SchemaNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

public class DropSchemaIT extends BaseClientManagedTimeIT {

    @Test
    public void testDropSchema() throws Exception {
        long ts = nextTimestamp();
        String schema = "TEST_SCHEMA";
        String tableName = "TEST";
        Properties props = new Properties();
        String ddl = "DROP SCHEMA " + schema;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute("CREATE SCHEMA " + schema);
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute("CREATE TABLE " + schema + "." + tableName + "(id INTEGER PRIMARY KEY)");
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 15));
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {

            try {
                conn.createStatement().execute(ddl);
                fail();
            } catch (SQLException e) {
                e.printStackTrace();
                assertEquals(e.getErrorCode(), SQLExceptionCode.CANNOT_MUTATE_SCHEMA.getErrorCode());
            }
            assertNotNull(admin.getNamespaceDescriptor(schema));
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts - 20));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute(ddl);
            fail();
        } catch (SchemaNotFoundException e) {
            // expected
        }
        assertNotNull(admin.getNamespaceDescriptor(schema));

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute("DROP TABLE " + schema + "." + tableName);
            //Dropping table manually because Drop_meta_attrib is not working
            admin.disableTable(TableName.valueOf(schema, tableName));
            admin.deleteTable(TableName.valueOf(schema, tableName));
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 50));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute(ddl);
            try {
                admin.getNamespaceDescriptor(schema);
                fail();
            } catch (NamespaceNotFoundException ne) {
                // expected
            }
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 60));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute("DROP SCHEMA IF EXISTS " + schema);
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 70));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            admin.createNamespace(NamespaceDescriptor.create(schema).build());
            conn.createStatement().execute("DROP SCHEMA IF EXISTS " + schema);
            assertNotNull(admin.getNamespaceDescriptor(schema));
        }
        admin.close();
    }

}
