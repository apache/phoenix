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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Map;

import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class ShortViewIndexIdIT extends BaseTest {
    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1);
        serverProps.put(QueryServices.LONG_VIEW_INDEX_ENABLED_ATTRIB, "false");
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(1);
        clientProps.put(QueryServices.LONG_VIEW_INDEX_ENABLED_ATTRIB, "false");
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
    }

    @Test
    public void testCreateLocalIndexWithData() throws Exception {
        String tableName = generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement()
                    .execute("CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, v INTEGER)");
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(1, 1)");
            conn.commit();
            conn.createStatement()
                    .execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v)");
            conn.commit();
            // this should return the expected data if the index was written correctly
            ResultSet rs = conn.createStatement()
                    .executeQuery("SELECT v FROM " + tableName + " WHERE v < 2");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            rs.close();
        }
    }

    @Test
    public void testUpsertIntoLocalIndex() throws Exception {
        String tableName = generateUniqueName();
        String index1 = "IDX_" + generateUniqueName();
        String index2 = "IDX_" + generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement()
                    .execute("CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, v1 INTEGER, v2 INTEGER)");
            conn.createStatement()
            .execute("CREATE LOCAL INDEX " + index1 + " ON " + tableName + "(v1)");
            conn.createStatement()
            .execute("CREATE LOCAL INDEX " + index2 + " ON " + tableName + "(v2)");
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(1, 2, 3)");
            conn.commit();
            ResultSet rs = conn.createStatement()
                    .executeQuery("SELECT * FROM " + index1);
            // we're expecting exactly one row mapping the column value to the key
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(1, rs.getInt(2));
            assertFalse(rs.next());
            rs.close();

            rs = conn.createStatement()
                    .executeQuery("SELECT * FROM " + index2);
            // we're expecting exactly one row mapping the column value to the key
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertEquals(1, rs.getInt(2));
            assertFalse(rs.next());
            rs.close();
        }
    }

}
