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

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.A_VALUE;
import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.C_VALUE;
import static org.apache.phoenix.util.TestUtil.E_VALUE;
import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.ROW4;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


@Category(NeedsOwnMiniClusterTest.class)
public class PointInTimeScanQueryIT extends BaseQueryIT {

    @Parameters(name="PointInTimeScanQueryIT_{index},columnEncoded={1}")
    public static synchronized Collection<Object> data() {
        return BaseQueryIT.allIndexesWithEncodedAndKeepDeleted();
    }

    public PointInTimeScanQueryIT(String idxDdl, boolean columnEncoded,
            boolean keepDeletedCells) throws Exception {
        super(idxDdl, columnEncoded, keepDeletedCells);

        // For this class we specifically want to run each test method with
        // fresh tables, it is expected to be slow
        initTables(idxDdl, columnEncoded, keepDeletedCells);
    }

    @Test
    public void testPointInTimeScan() throws Exception {
        // Override value that was set at creation time
        String url = getUrl();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String upsertStmt =
            "upsert into " + tableName +
                " (" +
                "    ORGANIZATION_ID, " +
                "    ENTITY_ID, " +
                "    A_INTEGER) " +
                "VALUES (?, ?, ?)";

        try (Connection upsertConn = DriverManager.getConnection(url, props)) {
            upsertConn.setAutoCommit(true); // Test auto commit
            PreparedStatement stmt = upsertConn.prepareStatement(upsertStmt);
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW4);
            stmt.setInt(3, 5);
            stmt.execute(); // should commit too
        }
        long upsert1Time = System.currentTimeMillis();
        long timeDelta = 100;
        Thread.sleep(timeDelta);

        try (Connection upsertConn = DriverManager.getConnection(url, props)) {
            upsertConn.setAutoCommit(true); // Test auto commit
            PreparedStatement stmt = upsertConn.prepareStatement(upsertStmt);
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW4);
            stmt.setInt(3, 9);
            stmt.execute(); // should commit too
        }

        long queryTime = upsert1Time + timeDelta / 2;
        String query = "SELECT organization_id, a_string AS a FROM "
            + tableName + " WHERE organization_id=? and a_integer = 5";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
            Long.toString(queryTime));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(tenantId, rs.getString(1));
            assertEquals(A_VALUE, rs.getString("a"));
            assertTrue(rs.next());
            assertEquals(tenantId, rs.getString(1));
            assertEquals(B_VALUE, rs.getString(2));
            assertFalse(rs.next());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPointInTimeLimitedScan() throws Exception {
        // Override value that was set at creation time
        String url = getUrl();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String upsertStmt =
            "upsert into " + tableName +
                " (" +
                "    ORGANIZATION_ID, " +
                "    ENTITY_ID, " +
                "    A_INTEGER) " +
                "VALUES (?, ?, ?)";
        try (Connection upsertConn = DriverManager.getConnection(url, props)) {
            upsertConn.setAutoCommit(true); // Test auto commit
            // Insert all rows at ts
            PreparedStatement stmt = upsertConn.prepareStatement(upsertStmt);
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW1);
            stmt.setInt(3, 6);
            stmt.execute(); // should commit too
        }
        long upsert1Time = System.currentTimeMillis();
        long timeDelta = 100;
        Thread.sleep(timeDelta);

        url = getUrl();
        try(Connection upsertConn = DriverManager.getConnection(url, props)) {
            upsertConn.setAutoCommit(true); // Test auto commit
            // Insert all rows at ts
            PreparedStatement stmt = upsertConn.prepareStatement(upsertStmt);
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW1);
            stmt.setInt(3, 0);
            stmt.execute(); // should commit too
        }

        long queryTime = upsert1Time + timeDelta  / 2;
        String query = "SELECT a_integer,b_string FROM " + tableName
            + " WHERE organization_id=? and a_integer <= 5 limit 2";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
            Long.toString(queryTime));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            List<List<Object>> expectedResultsA = Lists.newArrayList(
                Arrays.asList(2, C_VALUE),
                Arrays.asList(3, E_VALUE));
            List<List<Object>> expectedResultsB = Lists.newArrayList(
                Arrays.asList(5, C_VALUE),
                Arrays.asList(4, B_VALUE));
            // Since we're not ordering and we may be using a descending index, we don't
            // know which rows we'll get back.
            assertOneOfValuesEqualsResultSet(rs, expectedResultsA,
                expectedResultsB);
        }
    }

}
