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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.SchemaNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Maps;

@RunWith(Parameterized.class)
public class DropSchemaIT extends BaseClientManagedTimeIT {
    private String schema;
    
    @Shadower(classBeingShadowed = BaseClientManagedTimeIT.class)
    @BeforeClass 
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(1);
        // Drop the HBase table metadata for this test
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        // Must update config before starting server
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    public DropSchemaIT(String schema) {
        this.schema = schema;
    }

    @Parameters(name = "DropSchemaIT_schema={0}") // name is used by failsafe as file name in reports
    public static Collection<String> data() {
        return Arrays.asList("TEST_SCHEMA", "\"test_schema\"");
    }

    @Test
    public void testDropSchema() throws Exception {
        long ts = nextTimestamp();

        String tableName = "TEST";
        Properties props = new Properties();
        String normalizeSchemaIdentifier = SchemaUtil.normalizeIdentifier(schema);
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
            assertNotNull(admin.getNamespaceDescriptor(normalizeSchemaIdentifier));
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts - 20));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute(ddl);
            fail();
        } catch (SchemaNotFoundException e) {
            // expected
        }
        assertNotNull(admin.getNamespaceDescriptor(normalizeSchemaIdentifier));

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute("DROP TABLE " + schema + "." + tableName);
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 50));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute(ddl);
            try {
                admin.getNamespaceDescriptor(normalizeSchemaIdentifier);
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
            admin.createNamespace(NamespaceDescriptor.create(normalizeSchemaIdentifier).build());
            conn.createStatement().execute("DROP SCHEMA IF EXISTS " + schema);
            assertNotNull(admin.getNamespaceDescriptor(normalizeSchemaIdentifier));
            conn.createStatement().execute("CREATE SCHEMA " + schema);
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 80));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute("DROP SCHEMA " + schema);
        }
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute("DROP SCHEMA " + schema);
            fail();
        } catch (SQLException e) {
            assertEquals(e.getErrorCode(), SQLExceptionCode.SCHEMA_NOT_FOUND.getErrorCode());
        }
        admin.close();
    }
}
