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

import com.google.common.collect.Maps;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.end2end.Shadower;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.BaseTest;

import java.sql.*;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for Upsert Statement Compilation
 * testUpsertTruncChar() String should be truncated to max CHAR size
 */

public class UpsertQueryTest extends BaseTest {

    @Test
    public void testUpsertTruncChar() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String testTable = "phoenix572";
        String origStr = "abcde";
        String expectedStr = "abc";
        int    charSize = 3;
        String stmt;

        stmt = "CREATE TABLE " + testTable + "(\n" +
                "k1 VARCHAR NOT NULL PRIMARY KEY, v1 CHAR(" +
                charSize + "))";
        conn.createStatement().execute(stmt);

        conn.createStatement().executeUpdate("UPSERT INTO " + testTable +
                " values('x', '" + origStr + "')");
        conn.commit();

        stmt = "SELECT v1, length(v1) FROM " + testTable;
        ResultSet rs = conn.createStatement().executeQuery(stmt);

        assertTrue(rs.next());
        assertEquals(expectedStr, rs.getString(1));
        assertEquals(charSize, rs.getInt(2));

        stmt = "DROP TABLE " + testTable;
        conn.createStatement().execute(stmt);
        conn.close();
    }

    @BeforeClass
    @Shadower(classBeingShadowed = BaseTest.class)
    public static void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(3);
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @AfterClass
    public static void doTeardown() throws Exception {
        dropNonSystemTables();
    }
}
