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

import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.ROW2;
import static org.apache.phoenix.util.TestUtil.ROW3;
import static org.apache.phoenix.util.TestUtil.ROW4;
import static org.apache.phoenix.util.TestUtil.ROW5;
import static org.apache.phoenix.util.TestUtil.ROW6;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.ROW8;
import static org.apache.phoenix.util.TestUtil.ROW9;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(ParallelStatsDisabledTest.class)
public class TopNIT extends ParallelStatsDisabledIT {

    @Test
    public void testMultiOrderByExpr() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(), null);
        String query = "SELECT entity_id FROM " + tableName + " ORDER BY b_string, entity_id LIMIT 5";
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW1);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW4);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW7);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW5);

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    

    @Test
    public void testDescMultiOrderByExpr() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(), null);
        String query = "SELECT entity_id FROM  " + tableName + "  ORDER BY b_string || entity_id desc LIMIT 5";
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW9);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW6);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW3);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW8);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW5);

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    

    @Test
    public void testTopNDeleteAutoCommitOn() throws Exception {
        testTopNDelete(true);
    }
    
    @Test
    public void testTopNDeleteAutoCommitOff() throws Exception {
        testTopNDelete(false);
    }
    
    private void testTopNDelete(boolean autoCommit) throws Exception {
        String tenantId = getOrganizationId();
        String tableName =
                initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(),
                    null);
        String query = "DELETE FROM  " + tableName + "  ORDER BY b_string, entity_id LIMIT 5";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(autoCommit);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.execute();
            assertEquals(5, statement.getUpdateCount());
            if (!autoCommit) {
                conn.commit();
            }
            query =
                    "SELECT entity_id FROM  " + tableName + "  ORDER BY b_string, x_decimal nulls last, 8-a_integer LIMIT 5";
            statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW8, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW6, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW3, rs.getString(1));
            assertFalse(rs.next());
        }
    }
    

}
