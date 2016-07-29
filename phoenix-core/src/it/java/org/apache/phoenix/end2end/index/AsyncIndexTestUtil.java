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

import static org.apache.phoenix.query.QueryConstants.ASYNC_INDEX_INFO_QUERY;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class AsyncIndexTestUtil {
    public static void createTableAndLoadData(Statement stmt, String tableName) throws SQLException {
        String ddl = "CREATE TABLE " + tableName + " (ID INTEGER NOT NULL PRIMARY KEY, " +
                     "FNAME VARCHAR, LNAME VARCHAR)";
        
        stmt.execute(ddl);
        stmt.execute("UPSERT INTO " + tableName + " values(1, 'FIRST', 'F')");
        stmt.execute("UPSERT INTO " + tableName + " values(2, 'SECOND', 'S')");
    }

    public static void createAsyncIndex(Statement stmt, String indexName, String tableName) throws SQLException {
        stmt.execute("CREATE INDEX " + indexName + " ON " + tableName + "(FNAME) ASYNC");
    }

    public static void retryWithSleep(String tableName, int maxRetries, int sleepInSecs, Statement stmt) throws Exception {
        String personTableAsyncIndexInfoQuery = getPersonTableAsyncIndexInfoQuery(tableName);
        ResultSet rs = stmt.executeQuery(personTableAsyncIndexInfoQuery);
        // Wait for max of 5 retries with each retry of 5 sec sleep
        int retries = 0;
        while(retries <= maxRetries) {
            Thread.sleep(sleepInSecs * 1000);
            rs = stmt.executeQuery(personTableAsyncIndexInfoQuery);
            if (!rs.next()) {
                break;
            }
            retries++;
        }
    }
    
    public static String getPersonTableAsyncIndexInfoQuery(String tableName) {
        return ASYNC_INDEX_INFO_QUERY + " and DATA_TABLE_NAME='" + tableName + "'";
    }
}