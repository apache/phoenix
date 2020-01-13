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

import org.apache.phoenix.mapreduce.index.IndexUpgradeTool;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class IndexUpgradeToolIT extends BaseTest {

    public static final String
            VERIFY_COUNT_ASSERT_MESSAGE = "view-index count in system table doesn't match";

    @Test
    public void verifyViewAndViewIndexes() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        Map<String, String> props = Collections.emptyMap();
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));

        try (Connection conn = DriverManager.getConnection(getUrl(), new Properties())) {
            prepareForTest(conn, schemaName, tableName);
            IndexUpgradeTool iut = new IndexUpgradeTool();
            String viewQuery = iut.getViewSql(tableName, schemaName);
            ResultSet rs = conn.createStatement().executeQuery(viewQuery);
            int countViews = 0;
            List<String> views = new ArrayList<>();
            List<Integer> indexCount = new ArrayList<>();
            while (rs.next()) {
                views.add(rs.getString(1));
                countViews++;
            }
            Assert.assertEquals("view count in system table doesn't match", 2, countViews);
            for (int i = 0; i < views.size(); i++) {
                String viewName = SchemaUtil.getTableNameFromFullName(views.get(i));
                String viewIndexQuery = iut.getViewIndexesSql(viewName, schemaName, null);
                rs = conn.createStatement().executeQuery(viewIndexQuery);
                int indexes = 0;
                while (rs.next()) {
                    indexes++;
                }
                indexCount.add(indexes);
            }
            Assert.assertEquals(VERIFY_COUNT_ASSERT_MESSAGE, 2, (int) indexCount.get(0));
            Assert.assertEquals(VERIFY_COUNT_ASSERT_MESSAGE, 1, (int) indexCount.get(1));
        }
    }

    private void prepareForTest(Connection conn, String schemaName, String tableName)
            throws SQLException {
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        conn.createStatement().execute("CREATE TABLE "+fullTableName+" (id bigint NOT NULL "
                + "PRIMARY KEY, a.name varchar, sal bigint, address varchar)");

        for (int i = 0; i<2; i++) {
            String view = generateUniqueName();
            String fullViewName = SchemaUtil.getTableName(schemaName, view);
            conn.createStatement().execute("CREATE VIEW "+ fullViewName+ " (view_column varchar)"
                    + " AS SELECT * FROM " +fullTableName + " WHERE a.name = 'a'");
            for(int j=i; j<2; j++) {
                String index = generateUniqueName();
                conn.createStatement().execute("CREATE INDEX " + index + " ON "
                        + fullViewName + " (view_column)");
            }
        }
    }
}
