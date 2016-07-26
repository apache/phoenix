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
    private static final String PERSON_TABLE_NAME = "PERSON";
    private static final String PERSON_TABLE_NAME_WITH_SCHEMA = "TEST.PERSON";
    private static final String TEST_SCHEMA = "TEST";

    public static final String PERSON_TABLE_ASYNC_INDEX_INFO_QUERY = 
            ASYNC_INDEX_INFO_QUERY + " and DATA_TABLE_NAME='" + PERSON_TABLE_NAME 
            + "' and TABLE_SCHEM='" + TEST_SCHEMA + "'";

    public static void createTableAndLoadData(Statement stmt) throws SQLException {
        String ddl = "CREATE TABLE " + PERSON_TABLE_NAME_WITH_SCHEMA + " (ID INTEGER NOT NULL PRIMARY KEY, " +
                     "FNAME VARCHAR, LNAME VARCHAR)";
        
        stmt.execute(ddl);
        stmt.execute("UPSERT INTO " + PERSON_TABLE_NAME_WITH_SCHEMA + " values(1, 'FIRST', 'F')");
        stmt.execute("UPSERT INTO " + PERSON_TABLE_NAME_WITH_SCHEMA + " values(2, 'SECOND', 'S')");
    }

    public static void createAsyncIndex(Statement stmt) throws SQLException {
        stmt.execute("CREATE INDEX FNAME_INDEX ON " + PERSON_TABLE_NAME_WITH_SCHEMA + "(FNAME) ASYNC");
    }

    public static void retryWithSleep(int maxRetries, int sleepInSecs, Statement stmt) throws Exception {
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
}