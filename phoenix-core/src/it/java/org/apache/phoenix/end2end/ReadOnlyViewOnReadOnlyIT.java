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
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.UPDATE_CACHE_FREQUENCY;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class ReadOnlyViewOnReadOnlyIT extends BaseTenantSpecificViewIndexIT {
    private static final long DEFAULT_TTL_FOR_TEST = 86400;
    private Connection getTenantConnection(final String tenantId) throws Exception {
        Properties tenantProps = new Properties();
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getUrl(), tenantProps);
    }
    @Test
    public void testReadOnlyTenantViewOnReadOnly() throws Exception {

        try(Connection connection = DriverManager.getConnection(getUrl())) {
            PhoenixConnection conn = connection.unwrap(PhoenixConnection.class);
            //base table
            String tableName = generateUniqueName();
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS  " + tableName + "  ("
                    + " ID INTEGER NOT NULL,"
                    + " COL1 INTEGER NOT NULL,"
                    + " COL2 bigint NOT NULL,"
                    + " CREATED_DATE DATE,"
                    + " CREATION_TIME BIGINT,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (ID, COL1, COL2))"
                    + " TTL = " + DEFAULT_TTL_FOR_TEST
                    + "," + UPDATE_CACHE_FREQUENCY + " = 100000000"
                    + ", MULTI_TENANT = true");

            //global parent view
            String viewName = "VIEW_" + tableName + "_" + generateUniqueName();
            conn.createStatement().execute("CREATE VIEW " + viewName
                    + " (" + generateUniqueName() + " SMALLINT) as select * from "
                    + tableName + " where id > 1 "
                    + (false ? "TTL = 1000" : ""));

            //tenant child view
            String TENANT_ID = generateUniqueName();
            try (PhoenixConnection tenantConn = (PhoenixConnection) getTenantConnection(TENANT_ID))
            {
                final Statement tenantStmt = tenantConn.createStatement();
                tenantStmt.execute("CREATE VIEW " + "TENANT_VIEW_" + viewName + " AS SELECT * FROM " + viewName);

                assertEquals(PTable.ViewType.READ_ONLY, conn.getTable(viewName).getViewType());
                assertEquals(PTable.ViewType.READ_ONLY, tenantConn.getTable(
                        "TENANT_VIEW_" + viewName).getViewType());
            }
        }
    }

    @Test
    public void testReadOnlyViewOnReadOnly() throws Exception {
        try(Connection connection = DriverManager.getConnection(getUrl())) {
            PhoenixConnection conn = connection.unwrap(PhoenixConnection.class);
            //base table
            String tableName = generateUniqueName();
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS  " + tableName + "  ("
                    + " ID INTEGER NOT NULL,"
                    + " COL1 INTEGER NOT NULL,"
                    + " COL2 bigint NOT NULL,"
                    + " CREATED_DATE DATE,"
                    + " CREATION_TIME BIGINT,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (ID, COL1, COL2))"
                    + " TTL = " + DEFAULT_TTL_FOR_TEST + "," + UPDATE_CACHE_FREQUENCY + " = 100000000");

            //global parent view
            String viewName = "VIEW_" + tableName + "_" + generateUniqueName();
            conn.createStatement().execute("CREATE VIEW " + viewName
                    + " (" + generateUniqueName() + " SMALLINT) as select * from "
                    + tableName + " where id > 1 "
                    + (false ? "TTL = 1000" : ""));

            //global child view
            String childView = "VIEW_" + viewName + "_" + generateUniqueName();
            conn.createStatement().execute("CREATE VIEW " + childView + " AS SELECT * FROM " + viewName);

            assertEquals(PTable.ViewType.READ_ONLY, conn.getTable(viewName).getViewType());
            assertEquals(PTable.ViewType.READ_ONLY, conn.getTable(childView).getViewType());

        }
    }
}