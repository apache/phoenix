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

import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.coprocessor.TableViewFinderResult;
import org.apache.phoenix.coprocessor.ViewFinder;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class DropTableWithViewsIT extends SplitSystemCatalogIT {

    private final boolean isMultiTenant;
    private final boolean columnEncoded;
    private final String TENANT_SPECIFIC_URL1 = getUrl() + ';' + TENANT_ID_ATTRIB + "=" + TENANT1;

    public DropTableWithViewsIT(boolean isMultiTenant, boolean columnEncoded) {
        this.isMultiTenant = isMultiTenant;
        this.columnEncoded = columnEncoded;
    }

    @Parameters(name="DropTableWithViewsIT_multiTenant={0}, columnEncoded={1}") // name is used by failsafe as file name in reports
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {
                { false, false }, { false, true },
                { true, false }, { true, true } });
    }

    private String generateDDL(String format) {
        return generateDDL("", format);
    }

    private String generateDDL(String options, String format) {
        StringBuilder optionsBuilder = new StringBuilder(options);
        if (!columnEncoded) {
            if (optionsBuilder.length() != 0)
                optionsBuilder.append(",");
            optionsBuilder.append("COLUMN_ENCODED_BYTES=0");
        }
        if (isMultiTenant) {
            if (optionsBuilder.length() !=0 )
                optionsBuilder.append(",");
            optionsBuilder.append("MULTI_TENANT=true");
        }
        return String.format(format, isMultiTenant ? "TENANT_ID VARCHAR NOT NULL, " : "",
            isMultiTenant ? "TENANT_ID, " : "", optionsBuilder.toString());
    }

    @Test
    public void testDropTableWithChildViews() throws Exception {
        String baseTable = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn =
                        isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn) {
            String ddlFormat =
                    "CREATE TABLE IF NOT EXISTS " + baseTable + "  ("
                            + " %s PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR "
                            + " CONSTRAINT NAME_PK PRIMARY KEY (%s PK2)" + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));
            conn.commit();
            // Create a view tree (i.e., tree of views) with depth of 2 and fanout factor of 4
            for (int  i = 0; i < 4; i++) {
                String childView = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
                String childViewDDL = "CREATE VIEW " + childView + " AS SELECT * FROM " + baseTable;
                viewConn.createStatement().execute(childViewDDL);
                for (int j = 0; j < 4; j++) {
                    String grandChildView = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
                    String grandChildViewDDL = "CREATE VIEW " + grandChildView + " AS SELECT * FROM " + childView;
                    viewConn.createStatement().execute(grandChildViewDDL);
                }
            }
            // Drop the base table
            String dropTable = String.format("DROP TABLE IF EXISTS %s CASCADE", baseTable);
            conn.createStatement().execute(dropTable);

            // Wait for the tasks for dropping child views to complete. The depth of the view tree is 2, so we expect that
            // this will be done in two task handling runs, i.e., in tree task handling interval at most in general
            // by assuming that each non-root level will be processed in one interval. To be on the safe side, we will
            // wait at most 10 intervals.
            long halfTimeInterval = config.getLong(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB,
                    QueryServicesOptions.DEFAULT_TASK_HANDLING_INTERVAL_MS)/2;
            ResultSet rs = null;
            boolean timedOut = true;
            Thread.sleep(3 * halfTimeInterval);
            for (int i = 3; i < 20; i++) {
                rs = conn.createStatement().executeQuery("SELECT * " +
                                " FROM " + PhoenixDatabaseMetaData.SYSTEM_TASK_NAME +
                                " WHERE " + PhoenixDatabaseMetaData.TASK_TYPE + " = " +
                                PTable.TaskType.DROP_CHILD_VIEWS.getSerializedValue());
                Thread.sleep(halfTimeInterval);
                if (!rs.next()) {
                    timedOut = false;
                    break;
                }
            }
            if (timedOut) {
                fail("Drop child view task execution timed out!");
            }
            // Views should be dropped by now
            TableName linkTable = TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES);
            TableViewFinderResult childViewsResult = new TableViewFinderResult();
            ViewFinder.findAllRelatives(getUtility().getConnection().getTable(linkTable),
                    HConstants.EMPTY_BYTE_ARRAY,
                    SchemaUtil.getSchemaNameFromFullName(baseTable).getBytes(),
                    SchemaUtil.getTableNameFromFullName(baseTable).getBytes(),
                    PTable.LinkType.CHILD_TABLE,
                    childViewsResult);
            assertTrue(childViewsResult.getLinks().size() == 0);
            // There should not be any orphan views
            rs = conn.createStatement().executeQuery("SELECT * FROM " + PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME +
                    " WHERE " + PhoenixDatabaseMetaData.TABLE_SCHEM + " = '" + SCHEMA2 +"'");
            assertFalse(rs.next());
        }
    }
}
