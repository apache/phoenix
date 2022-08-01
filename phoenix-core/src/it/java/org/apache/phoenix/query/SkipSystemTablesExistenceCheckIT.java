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

package org.apache.phoenix.query;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.PHOENIX_JDBC_URL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


@Category(ParallelStatsDisabledTest.class)
@SuppressWarnings("deprecated")
public class SkipSystemTablesExistenceCheckIT extends ParallelStatsDisabledIT {

    @Test
    public void testTableResultIterator() throws Exception {
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL);
        PhoenixConnection phoenixConnection = conn.unwrap(PhoenixConnection.class);
        String tableName = generateUniqueName();

        conn.createStatement().execute("CREATE TABLE " + tableName
                + " (A UNSIGNED_LONG NOT NULL PRIMARY KEY, B VARCHAR(10))");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (1, 'A')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (2, 'B')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (3, 'C')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (4, 'D')");
        conn.commit();

        scanTable(conn, tableName);
        Properties props = new Properties();
        props.setProperty(QueryServices.SKIP_SYSTEM_TABLES_EXISTENCE_CHECK, "true");
        ConnectionQueryServicesImpl queryServices = ((ConnectionQueryServicesImpl)phoenixConnection.getQueryServices());
        phoenixConnection.close();
        queryServices.setInitialized(false);
        queryServices.init(PHOENIX_JDBC_URL, props);
        assertTrue(queryServices.isInitialized());
        conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        scanTable(conn, tableName);
    }

    private void scanTable(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT A, B FROM " + tableName + " ORDER BY A DESC";
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        ResultSet rs = stmt.executeQuery(sql);

        int cnt = 0;
        while ((rs.next())) {
            cnt++;
            assertTrue("too many results returned", cnt <= 4);
        }
        assertEquals(4, cnt);
    }
}
