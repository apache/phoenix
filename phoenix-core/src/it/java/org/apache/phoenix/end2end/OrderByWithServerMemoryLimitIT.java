/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class OrderByWithServerMemoryLimitIT extends BaseTest {

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.SERVER_SPOOL_THRESHOLD_BYTES_ATTRIB, Integer.toString(1));
        props.put(QueryServices.SERVER_ORDERBY_SPOOLING_ENABLED_ATTRIB,
            Boolean.toString(Boolean.FALSE));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testOrderByWithServerMemoryLimit() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String tableName = generateUniqueName();
            String ddl =
                    "CREATE TABLE " + tableName + "  (a_string varchar not null, col1 integer"
                            + "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            String dml = "UPSERT INTO " + tableName + " VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 40);
            stmt.execute();
            stmt.setString(1, "b");
            stmt.setInt(2, 20);
            stmt.execute();
            stmt.setString(1, "c");
            stmt.setInt(2, 30);
            stmt.execute();
            conn.commit();

            String query = "select * from " + tableName + " order by 2";
            ResultSet rs = conn.createStatement().executeQuery(query);
            try {
                rs.next();
                fail("Expected PhoenixIOException due to IllegalStateException");
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains(
                        "ERROR 101 (08000): Unexpected IO exception. ERROR 101 (08000): "
                                + "Queue full. Consider increasing memory threshold or "
                                + "spooling to disk"));
            }
        }
    }
}
