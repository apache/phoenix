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

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.NewerSchemaAlreadyExistsException;
import org.apache.phoenix.schema.SchemaAlreadyExistsException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;

public class CreateSchemaIT extends BaseClientManagedTimeIT {

    @Test
    public void testCreateSchema() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        String ddl = "CREATE SCHEMA TEST_SCHEMA";
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
                HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();) {
            conn.createStatement().execute(ddl);
            assertNotNull(admin.getNamespaceDescriptor("TEST_SCHEMA"));
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute(ddl);
            fail();
        } catch (SchemaAlreadyExistsException e) {
            // expected
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts - 20));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(ddl);
            fail();
        } catch (NewerSchemaAlreadyExistsException e) {
            // expected
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 50));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute("CREATE SCHEMA " + SchemaUtil.SCHEMA_FOR_DEFAULT_NAMESPACE);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SCHEMA_NOT_ALLOWED.getErrorCode(), e.getErrorCode());
        }
        try {
            conn.createStatement().execute("CREATE SCHEMA " + SchemaUtil.HBASE_NAMESPACE);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SCHEMA_NOT_ALLOWED.getErrorCode(), e.getErrorCode());
        }
        conn.close();
    }
}
