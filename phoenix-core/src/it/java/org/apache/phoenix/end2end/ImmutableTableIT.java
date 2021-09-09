/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

@Category(ParallelStatsDisabledTest.class)
public class ImmutableTableIT extends ParallelStatsDisabledIT {

    @Test
    public void testQueryWithMultipleColumnFamiliesAndSingleConditionForImmutableTable()
        throws Exception {
        final String tn = generateUniqueName();
        final String url = getUrl();
        try (Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("CREATE TABLE %s (" +
                "ID VARCHAR PRIMARY KEY," +
                "COL1 VARCHAR," +
                "COL2 VARCHAR" +
                ") IMMUTABLE_ROWS = TRUE", tn));
            stmt.execute(String.format("UPSERT INTO %s VALUES ('id0', '0', 'a')", tn));
            stmt.execute(String.format("UPSERT INTO %s VALUES ('id1', '1', NULL)", tn));
            stmt.execute(String.format("UPSERT INTO %s VALUES ('id2', '2', 'b')", tn));
            stmt.execute(String.format("UPSERT INTO %s VALUES ('id3', '3', NULL)", tn));
            stmt.execute(String.format("UPSERT INTO %s VALUES ('id4', '4', 'c')", tn));
            stmt.execute(String.format("UPSERT INTO %s VALUES ('id5', '5', NULL)", tn));
            stmt.execute(String.format("UPSERT INTO %s VALUES ('id6', '6', 'd')", tn));
            stmt.execute(String.format("UPSERT INTO %s VALUES ('id7', '7', NULL)", tn));
            stmt.execute(String.format("UPSERT INTO %s VALUES ('id8', '8', 'e')", tn));
            stmt.execute(String.format("UPSERT INTO %s VALUES ('id9', '9', NULL)", tn));
            conn.commit();

            try (ResultSet rs = stmt.executeQuery(String.format(
                "SELECT COL1 FROM %s WHERE COL2 IS NOT NULL", tn))) {
                int count = 0;
                while (rs.next()) {
                  count++;
                }
                assertEquals(5, count);
            }
        }
    }

    @Test
    public void testQueryWithMultipleColumnFamiliesAndMultipleConditionsForImmutableTable()
        throws Exception {
        final String tn = generateUniqueName();
        final String url = getUrl();
        try (Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("CREATE TABLE %s (" +
                "ID VARCHAR PRIMARY KEY," +
                "COL1 VARCHAR," +
                "COL2 VARCHAR," +
                "COL3 VARCHAR" +
                ") IMMUTABLE_ROWS = TRUE", tn));
            stmt.execute(String.format("UPSERT INTO %s VALUES ('id0', '0', '0', 'a')", tn));
            stmt.execute(String.format("UPSERT INTO %s VALUES ('id1', '1', '0', NULL)", tn));
            stmt.execute(String.format("UPSERT INTO %s VALUES ('id2', '2', '0', 'b')", tn));
            stmt.execute(String.format("UPSERT INTO %s VALUES ('id3', '3', '0', NULL)", tn));
            stmt.execute(String.format("UPSERT INTO %s VALUES ('id4', '4', '0', 'c')", tn));
            stmt.execute(String.format("UPSERT INTO %s VALUES ('id5', '5', '0', NULL)", tn));
            stmt.execute(String.format("UPSERT INTO %s VALUES ('id6', '6', '0', 'd')", tn));
            stmt.execute(String.format("UPSERT INTO %s VALUES ('id7', '7', '0', NULL)", tn));
            stmt.execute(String.format("UPSERT INTO %s VALUES ('id8', '8', '0', 'e')", tn));
            stmt.execute(String.format("UPSERT INTO %s VALUES ('id9', '9', '0', NULL)", tn));
            conn.commit();

            try (ResultSet rs = stmt.executeQuery(String.format(
                "SELECT COL1 FROM %s WHERE COL3 IS NOT NULL AND COL2='0'", tn))) {
                int count = 0;
                while (rs.next()) {
                  count++;
                }
                assertEquals(5, count);
            }
        }
    }
}
