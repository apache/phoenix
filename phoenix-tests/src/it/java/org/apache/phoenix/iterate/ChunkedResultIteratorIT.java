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

package org.apache.phoenix.iterate;

import static org.apache.phoenix.util.TestUtil.PHOENIX_JDBC_URL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableRef;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(ParallelStatsDisabledTest.class)
@SuppressWarnings("deprecated")
public class ChunkedResultIteratorIT
        extends ParallelStatsDisabledIT {

    @Test
    public void testChunked() throws Exception {
        Properties props = new Properties();
        props.setProperty(QueryServices.RENEW_LEASE_ENABLED, "false");
        props.setProperty(QueryServices.SCAN_RESULT_CHUNK_SIZE, "2");
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();

        conn.createStatement().execute("CREATE TABLE " + tableName
                + " (A UNSIGNED_LONG NOT NULL PRIMARY KEY, B VARCHAR(10))");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (1, 'A')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (2, 'B')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (3, 'C')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (4, 'D')");
        conn.commit();


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
