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
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.SchemaAlreadyExistsException;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class CreateSchemaIT extends ParallelStatsDisabledIT {

    @Test
    public void testCreateSchema() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        String schemaName = generateUniqueName();
        String schemaName1 = schemaName.toLowerCase();
        String schemaName2 = schemaName.toLowerCase();
        // Create unique name schema and verify that it exists
        // ddl1 should create lowercase schemaName since it is passed in with double-quotes
        // ddl2 should create uppercase schemaName since Phoenix upper-cases identifiers without quotes
        // Both the statements should succeed
        String ddl1 = "CREATE SCHEMA \"" + schemaName1 + "\"";
        String ddl2 = "CREATE SCHEMA " + schemaName2;
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
                Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();) {
            conn.createStatement().execute(ddl1);
            assertTrue(ClientUtil.isHBaseNamespaceAvailable(admin, schemaName1));
            conn.createStatement().execute(ddl2);
            assertTrue(ClientUtil.isHBaseNamespaceAvailable(admin, schemaName2.toUpperCase()));
        }
        // Try creating it again and verify that it throws SchemaAlreadyExistsException
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(ddl1);
            fail();
        } catch (SchemaAlreadyExistsException e) {
            // expected
        }
        // See PHOENIX-4424
        // Create schema DEFAULT and HBASE (Should allow since they are upper-cased) and verify that it exists
        // Create schema default and hbase and it should fail
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();) {

            // default is a SQL keyword, hence it should always be passed in double-quotes
            try {
                conn.createStatement().execute("CREATE SCHEMA \""
                        + SchemaUtil.SCHEMA_FOR_DEFAULT_NAMESPACE + "\"");
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.SCHEMA_NOT_ALLOWED.getErrorCode(), e.getErrorCode());
            }

            try {
                conn.createStatement().execute("CREATE SCHEMA \""
                        + SchemaUtil.HBASE_NAMESPACE + "\"");
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.SCHEMA_NOT_ALLOWED.getErrorCode(), e.getErrorCode());
            }

            // default is a SQL keyword, hence it should always be passed in double-quotes
            conn.createStatement().execute("CREATE SCHEMA \""
                    + SchemaUtil.SCHEMA_FOR_DEFAULT_NAMESPACE.toUpperCase() + "\"");
            conn.createStatement().execute("CREATE SCHEMA \""
                    + SchemaUtil.HBASE_NAMESPACE.toUpperCase() + "\"");

            assertTrue(ClientUtil.isHBaseNamespaceAvailable(admin,
                SchemaUtil.SCHEMA_FOR_DEFAULT_NAMESPACE.toUpperCase()));
            assertTrue(ClientUtil.isHBaseNamespaceAvailable(admin,
                SchemaUtil.HBASE_NAMESPACE.toUpperCase()));
        }
    }
}