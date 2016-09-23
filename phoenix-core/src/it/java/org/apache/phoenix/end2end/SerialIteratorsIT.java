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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

public class SerialIteratorsIT extends ParallelStatsDisabledIT {
    private String tableName = generateUniqueName();
    private final String[] strings = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p",
            "q", "r", "s", "t", "u", "v", "w", "x", "y", "z" };
    private final String ddl = "CREATE TABLE " + tableName + " (t_id VARCHAR NOT NULL,\n" + "k1 INTEGER NOT NULL,\n"
            + "k2 INTEGER NOT NULL,\n" + "C3.k3 INTEGER,\n" + "C2.v1 VARCHAR,\n"
            + "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2)) SPLIT ON ('e','i','o')";

    private static Connection getConnection() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.toString(false));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        return conn;
    }
    
    @Test
    public void testConcatenatingSerialIterators() throws Exception {
        Connection conn;
        conn = getConnection();
        createTestTable(getUrl(), ddl);
        initTableValues(conn);
        String query = "SELECT t_id from " + tableName + " order by t_id desc limit " + 10;
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        ResultSet rs = stmt.executeQuery(query);
        int i = 25;
        while (i >= 16) {
            assertTrue(rs.next());
            assertEquals(strings[i--], rs.getString(1));
        }
        query = "SELECT t_id from " + tableName + " order by t_id limit " + 10;
        stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        rs = stmt.executeQuery(query);
        i = 0;
        while (i < 10) {
            assertTrue(rs.next());
            assertEquals(strings[i++], rs.getString(1));
        }
        conn.close();
    }
    
    private void initTableValues(Connection conn) throws SQLException {
        for (int i = 0; i < 26; i++) {
            conn.createStatement().execute("UPSERT INTO " + tableName + " values('" + strings[i] + "'," + i + ","
                    + (i + 1) + "," + (i + 2) + ",'" + strings[25 - i] + "')");
        }
        conn.commit();
    }
    
}
