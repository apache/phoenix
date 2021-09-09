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
import org.apache.hadoop.hbase.client.Admin;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.SchemaNotFoundException;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class DropSchemaIT extends BaseTest {
    private String schema;
    
    public DropSchemaIT(String schema) {
        this.schema = schema;
    }

    @BeforeClass 
    public static synchronized void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(1);
        // Drop the HBase table metadata for this test
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        // Must update config before starting server
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }


    @Parameters(name = "DropSchemaIT_schema={0}") // name is used by failsafe as file name in reports
    public static synchronized Collection<String> data() {
        return Arrays.asList(generateUniqueName().toUpperCase(), "\"" + generateUniqueName().toLowerCase() + "\"");
    }

    @Test
    public void testDropSchema() throws Exception {
        String tableName = generateUniqueName();
        Properties props = new Properties();
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        String normalizeSchemaIdentifier = SchemaUtil.normalizeIdentifier(schema);
        String ddl = "DROP SCHEMA " + schema;
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            try {
                conn.createStatement().execute(ddl);
                fail();
            } catch (SchemaNotFoundException e) {
                // expected
            }
            conn.createStatement().execute("CREATE SCHEMA " + schema);
            conn.createStatement().execute("CREATE TABLE " + schema + "." + tableName + "(id INTEGER PRIMARY KEY)");
            try {
                conn.createStatement().execute(ddl);
                fail();
            } catch (SQLException e) {
                assertEquals(e.getErrorCode(), SQLExceptionCode.CANNOT_MUTATE_SCHEMA.getErrorCode());
            }
            assertNotNull(admin.getNamespaceDescriptor(normalizeSchemaIdentifier));

            conn.createStatement().execute("DROP TABLE " + schema + "." + tableName);
            conn.createStatement().execute(ddl);
            try {
                admin.getNamespaceDescriptor(normalizeSchemaIdentifier);
                fail();
            } catch (NamespaceNotFoundException ne) {
                // expected
            }
            
            conn.createStatement().execute("DROP SCHEMA IF EXISTS " + schema);
            
            admin.createNamespace(NamespaceDescriptor.create(normalizeSchemaIdentifier).build());
            conn.createStatement().execute("DROP SCHEMA IF EXISTS " + schema);
            assertNotNull(admin.getNamespaceDescriptor(normalizeSchemaIdentifier));
            conn.createStatement().execute("CREATE SCHEMA " + schema);
            conn.createStatement().execute("DROP SCHEMA " + schema);
            try {
                conn.createStatement().execute("DROP SCHEMA " + schema);
                fail();
            } catch (SQLException e) {
                assertEquals(e.getErrorCode(), SQLExceptionCode.SCHEMA_NOT_FOUND.getErrorCode());
            }
        }
    }
}
