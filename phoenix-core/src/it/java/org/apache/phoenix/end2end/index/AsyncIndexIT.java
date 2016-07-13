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
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ASYNC_CREATED_DATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_FAMILY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class AsyncIndexIT extends BaseTest {

    private static final String PERSON_TABLE_NAME = "PERSON";
    private static final String PERSON_TABLE_NAME_WITH_SCHEMA = "TEST.PERSON";
    private static final String TEST_SCHEMA = "TEST";

    private static final String PERSON_TABLE_ASYNC_INDEX_INFO_QUERY = "SELECT "
            + DATA_TABLE_NAME + ", " + TABLE_SCHEM + ", "
            + TABLE_NAME + " FROM " + SYSTEM_CATALOG_SCHEMA + ".\""
            + SYSTEM_CATALOG_TABLE + "\""
            + " (" + ASYNC_CREATED_DATE + " "
            + PDate.INSTANCE.getSqlTypeName() + ") " + " WHERE "
            + COLUMN_NAME + " IS NULL and " + COLUMN_FAMILY + " IS NULL  and "
            + ASYNC_CREATED_DATE + " IS NOT NULL and "
            + TABLE_TYPE + " = '" + PTableType.INDEX.getSerializedValue()
            + "' and DATA_TABLE_NAME='" + PERSON_TABLE_NAME 
            + "' and TABLE_SCHEM='" + TEST_SCHEMA + "' and "
            + PhoenixDatabaseMetaData.INDEX_STATE + " = '" 
            + PIndexState.BUILDING.getSerializedValue() + "'";

    private void dropTable(Statement stmt) throws SQLException, IOException {
        stmt.execute("DROP TABLE IF EXISTS " + PERSON_TABLE_NAME_WITH_SCHEMA);
    }

    private void createTableAndLoadData(Statement stmt) throws SQLException {
        String ddl = "CREATE TABLE " + PERSON_TABLE_NAME_WITH_SCHEMA + " (ID INTEGER NOT NULL PRIMARY KEY, " +
                     "FNAME VARCHAR, LNAME VARCHAR)";
        
        stmt.execute(ddl);
        stmt.execute("UPSERT INTO " + PERSON_TABLE_NAME_WITH_SCHEMA + " values(1, 'FIRST', 'F')");
        stmt.execute("UPSERT INTO " + PERSON_TABLE_NAME_WITH_SCHEMA + " values(2, 'SECOND', 'S')");
    }

    private void createAsyncIndex(Statement stmt) throws SQLException {
        stmt.execute("CREATE INDEX FNAME_INDEX ON " + PERSON_TABLE_NAME_WITH_SCHEMA + "(FNAME) ASYNC");
    }
    
    private void dropAsyncIndex(Statement stmt) throws SQLException {
        stmt.execute("DROP INDEX IF EXISTS FNAME_INDEX ON " + PERSON_TABLE_NAME_WITH_SCHEMA);
    }

    @After
    public void tearDown() throws Exception {
        tearDownMiniCluster();
    }

    private void retryWithSleep(int maxRetries, int sleepInSecs, Statement stmt) throws Exception {
        ResultSet rs = stmt.executeQuery(PERSON_TABLE_ASYNC_INDEX_INFO_QUERY);
        // Wait for max of 5 retries with each retry of 5 sec sleep
        int retries = 0;
        while(retries <= maxRetries) {
            Thread.sleep(sleepInSecs * 1000);
            rs = stmt.executeQuery(PERSON_TABLE_ASYNC_INDEX_INFO_QUERY);
            if (!rs.next()) {
                break;
            }
            retries++;
        }
    }
    
    @Test
    public void testAsyncIndexBuilderNonDistributed() throws Exception {
        setUpTestDriver(ReadOnlyProps.EMPTY_PROPS);
        Connection conn = DriverManager.getConnection(getUrl());
        Statement stmt = conn.createStatement();
        createTableAndLoadData(stmt);
        createAsyncIndex(stmt);

        ResultSet rs = stmt.executeQuery(PERSON_TABLE_ASYNC_INDEX_INFO_QUERY);
        assertTrue(rs.next());

        retryWithSleep(5, 5, stmt);

        rs = stmt.executeQuery(PERSON_TABLE_ASYNC_INDEX_INFO_QUERY);
        assertFalse(rs.next());

        dropAsyncIndex(stmt);
        dropTable(stmt);
    }
    
    @Test
    public void testAsyncIndexBuilderNonDistributedMapreduceYarn() throws Exception {
        Map<String,String> props = new HashMap<>();
        props.put(QueryServices.MAPRED_FRAMEWORK_NAME, "yarn");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
        
        Connection conn = DriverManager.getConnection(getUrl());
        Statement stmt = conn.createStatement();
        createTableAndLoadData(stmt);
        createAsyncIndex(stmt);

        ResultSet rs = stmt.executeQuery(PERSON_TABLE_ASYNC_INDEX_INFO_QUERY);
        assertTrue(rs.next());

        retryWithSleep(5, 5, stmt);

        rs = stmt.executeQuery(PERSON_TABLE_ASYNC_INDEX_INFO_QUERY);
        assertFalse(rs.next());

        dropAsyncIndex(stmt);
        dropTable(stmt);
    }

    @Test
    public void testAsyncIndexBuilderDistributed() throws Exception {
        Map<String,String> props = new HashMap<>();
        props.put(QueryServices.HBASE_CLUSTER_DISTRIBUTED_ATTRIB, "true");
        props.put(QueryServices.MAPRED_FRAMEWORK_NAME, "yarn");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
        
        Connection conn = DriverManager.getConnection(getUrl());
        Statement stmt = conn.createStatement();
        createTableAndLoadData(stmt);
        createAsyncIndex(stmt);

        ResultSet rs = stmt.executeQuery(PERSON_TABLE_ASYNC_INDEX_INFO_QUERY);
        assertTrue(rs.next());

        retryWithSleep(5, 5, stmt);

        rs = stmt.executeQuery(PERSON_TABLE_ASYNC_INDEX_INFO_QUERY);
        assertTrue(rs.next());

        dropAsyncIndex(stmt);
        dropTable(stmt);
    }
}
