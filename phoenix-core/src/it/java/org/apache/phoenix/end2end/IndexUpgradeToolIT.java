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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.mapreduce.index.IndexUpgradeTool;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category(ParallelStatsDisabledTest.class)
public class IndexUpgradeToolIT extends ParallelStatsDisabledIT {

    public static final String
            VERIFY_COUNT_ASSERT_MESSAGE = "view-index count in system table doesn't match";
    private final boolean multiTenant;
    private String tenantId = null;

    public IndexUpgradeToolIT(boolean multiTenant) {
        this.multiTenant = multiTenant;
    }

    @Parameters(name="isMultiTenant = {0}")
    public static synchronized Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {
                { true },{ false }
        });
    }

    @Test
    public void verifyViewAndViewIndexes() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        if (multiTenant) {
            tenantId = generateUniqueName();
            props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            prepareForTest(conn, schemaName, tableName);
            String viewQuery = IndexUpgradeTool.getViewSql(tableName, schemaName);
            ResultSet rs = conn.createStatement().executeQuery(viewQuery);
            List<String> views = new ArrayList<>();
            List<String> tenants = new ArrayList<>();
            while (rs.next()) {
                //1st column has the view name and 2nd column has the Tenant ID
                views.add(rs.getString(1));
                if(multiTenant) {
                    Assert.assertNotNull(rs.getString(2));
                }
                tenants.add(rs.getString(2));
            }
            Assert.assertEquals("view count in system table doesn't match", 2, views.size());

            for (int i = 0; i < views.size(); i++) {
                String viewName = SchemaUtil.getTableNameFromFullName(views.get(i));
                String viewIndexQuery = IndexUpgradeTool.getViewIndexesSql(viewName, schemaName,
                        tenants.get(i));
                rs = conn.createStatement().executeQuery(viewIndexQuery);
                int indexes = 0;
                while (rs.next()) {
                    indexes++;
                }
                // first (i=0) TSV has 2 indexes, and second(i=1) TSV has 1 index.
                Assert.assertEquals(VERIFY_COUNT_ASSERT_MESSAGE, 2-i, indexes);
            }
        }
    }

    private void prepareForTest(Connection conn, String schemaName, String tableName)
            throws SQLException {
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        Connection globalConn = conn;
        if (multiTenant) {
            Properties prop = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            globalConn = DriverManager.getConnection(getUrl(), prop);
        }
        globalConn.createStatement().execute("CREATE TABLE " + fullTableName + " (" + (
                multiTenant ? "TENANT_ID VARCHAR(15) NOT NULL, " : "") + "id bigint NOT NULL,"
                + " a.name varchar, sal bigint, address varchar CONSTRAINT PK_1 PRIMARY KEY "
                + "(" + (multiTenant ? "TENANT_ID, " : "") + "ID)) "
                + (multiTenant ? "MULTI_TENANT=true" : ""));

        for (int i = 0; i<2; i++) {
            String view = generateUniqueName();
            String fullViewName = SchemaUtil.getTableName(schemaName, view);
            conn.createStatement().execute("CREATE VIEW "+ fullViewName
                    + " AS SELECT * FROM " +fullTableName + " WHERE a.name = 'a'");
            for(int j=i; j<2; j++) {
                String index = generateUniqueName();
                conn.createStatement().execute("CREATE INDEX " + index + " ON "
                        + fullViewName + " (address)");
            }
        }
    }
}
