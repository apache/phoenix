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
package org.apache.phoenix.end2end.join;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.end2end.join.HashJoinCacheIT.InvalidateHashCache;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class HashJoinPersistentCacheIT extends BaseJoinIT {

    @Override
    protected String getTableName(Connection conn, String virtualName) throws Exception {
        String realName = super.getTableName(conn, virtualName);
        TestUtil.addCoprocessor(conn, SchemaUtil.normalizeFullTableName(realName),
                InvalidateHashCache.class);
        return realName;
    }

    @Test
    public void testPersistentCache() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        createTestTable(getUrl(),
                "CREATE TABLE IF NOT EXISTS states (state CHAR(2) " +
                "NOT NULL, name VARCHAR NOT NULL CONSTRAINT my_pk PRIMARY KEY (state, name))");
        createTestTable(getUrl(),
                "CREATE TABLE IF NOT EXISTS cities (state CHAR(2) " +
                 "NOT NULL, city VARCHAR NOT NULL, population BIGINT " +
                  "CONSTRAINT my_pk PRIMARY KEY (state, city))");

        conn.prepareStatement(
                "UPSERT INTO states VALUES ('CA', 'California')").executeUpdate();
        conn.prepareStatement(
                "UPSERT INTO states VALUES ('AZ', 'Arizona')").executeUpdate();
        conn.prepareStatement(
                "UPSERT INTO cities VALUES ('CA', 'San Francisco', 50000)").executeUpdate();
        conn.prepareStatement(
                "UPSERT INTO cities VALUES ('CA', 'Sacramento', 3000)").executeUpdate();
        conn.prepareStatement(
                "UPSERT INTO cities VALUES ('AZ', 'Phoenix', 20000)").executeUpdate();
        conn.commit();

        /* First, run query without using the persistent cache. This should return
        * different results after an UPSERT takes place.
        */
        ResultSet rs = conn.prepareStatement(
                "SELECT SUM(population) FROM states s "
                +"JOIN cities c ON c.state = s.state").executeQuery();
        rs.next();
        int population1 = rs.getInt(1);

        conn.prepareStatement("UPSERT INTO cities VALUES ('CA', 'Mt View', 1500)").executeUpdate();
        conn.commit();
        rs = conn.prepareStatement(
                "SELECT SUM(population) FROM states s " +
                 "JOIN cities c ON c.state = s.state").executeQuery();
        rs.next();
        int population2 = rs.getInt(1);

        assertEquals(73000, population1);
        assertEquals(74500, population2);

        /* Second, run query using the persistent cache. This should return the
        * same results after an UPSERT takes place.
        */
        rs = conn.prepareStatement(
                "SELECT /*+ USE_PERSISTENT_CACHE */ SUM(population) FROM states s " +
                 "JOIN cities c ON c.state = s.state").executeQuery();
        rs.next();
        int population3 = rs.getInt(1);

        conn.prepareStatement(
                "UPSERT INTO cities VALUES ('CA', 'Palo Alto', 2000)").executeUpdate();
        conn.commit();

        rs = conn.prepareStatement(
                "SELECT /*+ USE_PERSISTENT_CACHE */ SUM(population) " +
                 "FROM states s JOIN cities c ON c.state = s.state").executeQuery();
        rs.next();
        int population4 = rs.getInt(1);
        rs = conn.prepareStatement(
                "SELECT SUM(population) FROM states s JOIN cities c ON c.state = s.state")
                .executeQuery();
        rs.next();
        int population5 = rs.getInt(1);

        assertEquals(74500, population3);
        assertEquals(74500, population4);
        assertEquals(76500, population5);

        /* Let's make sure caches can be used across queries. We'll set up a
        * cache, and make sure it is used on two different queries with the
        * same subquery.
        */

        String sumQueryCached = "SELECT /*+ USE_PERSISTENT_CACHE */ SUM(population) " +
                "FROM cities c JOIN (SELECT state FROM states WHERE state LIKE 'C%') sq " +
                "ON sq.state = c.state";
        String distinctQueryCached = "SELECT /*+ USE_PERSISTENT_CACHE */ " +
                "COUNT(DISTINCT(c.city)) FROM cities c " +
                "JOIN (SELECT state FROM states WHERE state LIKE 'C%') sq " +
                "ON sq.state = c.state";
        String sumQueryUncached = sumQueryCached.replace(
                "/*+ USE_PERSISTENT_CACHE */", "");
        String distinctQueryUncached = distinctQueryCached.replace(
                "/*+ USE_PERSISTENT_CACHE */", "");

        rs = conn.prepareStatement(sumQueryCached).executeQuery();
        rs.next();
        int population6 = rs.getInt(1);
        rs = conn.prepareStatement(distinctQueryCached).executeQuery();
        rs.next();
        int distinct1 = rs.getInt(1);
        assertEquals(4, distinct1);

        // Add a new city that matches the queries. This should not affect results
        // using persistent caching.
        conn.prepareStatement("UPSERT INTO states VALUES ('CO', 'Colorado')").executeUpdate();
        conn.prepareStatement("UPSERT INTO cities VALUES ('CO', 'Denver', 6000)").executeUpdate();
        conn.commit();

        rs = conn.prepareStatement(sumQueryCached).executeQuery();
        rs.next();
        int population7 = rs.getInt(1);
        assertEquals(population6, population7);
        rs = conn.prepareStatement(distinctQueryCached).executeQuery();
        rs.next();
        int distinct2 = rs.getInt(1);
        assertEquals(distinct1, distinct2);

        // Finally, make sure uncached queries give up to date results
        rs = conn.prepareStatement(sumQueryUncached).executeQuery();
        rs.next();
        int population8 = rs.getInt(1);
        assertEquals(population8, 62500);
        rs = conn.prepareStatement(distinctQueryUncached).executeQuery();
        rs.next();
        int distinct3 = rs.getInt(1);
        assertEquals(5, distinct3);
    }
}
