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

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category(NeedsOwnMiniClusterTest.class)
public class AlterParentTableWithSysCatRollbackIT extends BaseTest {

    private String getJdbcUrl() {
        return "jdbc:phoenix:localhost:" + getUtility().getZkCluster().getClientPort()
            + ":/hbase";
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1);
        serverProps.put(QueryServices.ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK, "true");
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
            new ReadOnlyProps(Collections.emptyIterator()));
    }

    @Test
    public void testAddColumnOnParentTableView() throws Exception {
        try (Connection conn = DriverManager.getConnection(getJdbcUrl())) {
            String parentTableName = SchemaUtil.getTableName(generateUniqueName(),
                generateUniqueName());
            String parentViewName = SchemaUtil.getTableName(generateUniqueName(),
                generateUniqueName());
            String childViewName = SchemaUtil.getTableName(generateUniqueName(),
                generateUniqueName());
            // create parent table
            String ddl = "CREATE TABLE " + parentTableName
                + " (col1 INTEGER NOT NULL, col2 INTEGER " + "CONSTRAINT pk PRIMARY KEY (col1))";
            conn.createStatement().execute(ddl);

            // create view from table
            ddl = "CREATE VIEW " + parentViewName + " AS SELECT * FROM " + parentTableName;
            conn.createStatement().execute(ddl);
            try {
                ddl = "ALTER TABLE " + parentTableName + " ADD col4 INTEGER";
                conn.createStatement().execute(ddl);
                fail("ALTER TABLE should not be allowed on parent table");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }

            // create child view from above view
            ddl = "CREATE VIEW " + childViewName + "(col3 INTEGER) AS SELECT * FROM "
                + parentViewName;
            conn.createStatement().execute(ddl);
            try {
                ddl = "ALTER VIEW " + parentViewName + " ADD col4 INTEGER";
                conn.createStatement().execute(ddl);
                fail("ALTER VIEW should not be allowed on parent view");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }

            // alter child view with add column should be allowed
            ddl = "ALTER VIEW " + childViewName + " ADD col4 INTEGER";
            conn.createStatement().execute(ddl);
        }
    }

    @Test
    public void testDropColumnOnParentTableView() throws Exception {
        try (Connection conn = DriverManager.getConnection(getJdbcUrl())) {
            String parentTableName = SchemaUtil.getTableName(generateUniqueName(),
              generateUniqueName());
            String parentViewName = SchemaUtil.getTableName(generateUniqueName(),
              generateUniqueName());
            String childViewName = SchemaUtil.getTableName(generateUniqueName(),
              generateUniqueName());
            // create parent table
            String ddl = "CREATE TABLE " + parentTableName
                + " (col1 INTEGER NOT NULL, col2 INTEGER, col3 VARCHAR "
                + "CONSTRAINT pk PRIMARY KEY (col1))";
            conn.createStatement().execute(ddl);

            // create view from table
            ddl = "CREATE VIEW " + parentViewName + " AS SELECT * FROM " + parentTableName;
            conn.createStatement().execute(ddl);
            try {
                ddl = "ALTER TABLE " + parentTableName + " DROP COLUMN col2";
                conn.createStatement().execute(ddl);
                fail("ALTER TABLE DROP COLUMN should not be allowed on parent table");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }

            // create child view from above view
            ddl = "CREATE VIEW " + childViewName + "(col5 INTEGER) AS SELECT * FROM "
                + parentViewName;
            conn.createStatement().execute(ddl);
            try {
                ddl = "ALTER VIEW " + parentViewName + " DROP COLUMN col2";
                conn.createStatement().execute(ddl);
                fail("ALTER VIEW DROP COLUMN should not be allowed on parent view");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }

            // alter child view with drop column should be allowed
            ddl = "ALTER VIEW " + childViewName + " DROP COLUMN col2";
            conn.createStatement().execute(ddl);
        }
    }
}