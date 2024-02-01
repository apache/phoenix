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
package org.apache.phoenix.end2end.join;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class SortMergeJoinNoSpoolingIT extends SortMergeJoinNoIndexIT {

    public SortMergeJoinNoSpoolingIT(String[] indexDDL, String[] plans) {
        super(indexDDL, plans);
    }

    @Parameters(name = "SortMergeJoinNoSpoolingIT_{index}") // name is used by failsafe as file name
                                                            // in reports
    public static synchronized Collection<Object> data() {
        return SortMergeJoinNoIndexIT.data();
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.CLIENT_JOIN_SPOOLING_ENABLED_ATTRIB,
            Boolean.toString(Boolean.FALSE));
        props.put(QueryServices.CLIENT_SPOOL_THRESHOLD_BYTES_ATTRIB,
            Integer.toString(10 * 1000 * 1000));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testJoinWithMemoryLimit() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.put(QueryServices.CLIENT_SPOOL_THRESHOLD_BYTES_ATTRIB, Integer.toString(1));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String tableName1 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
            String tableName2 = getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME);
            String query =
                    "SELECT /*+ USE_SORT_MERGE_JOIN*/ item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM "
                            + tableName1 + " item JOIN " + tableName2
                            + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" ORDER BY \"item_id\"";

            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            try {
                rs.next();
                fail("Expected PhoenixIOException due to IllegalStateException");
            } catch (PhoenixIOException e) {
                assertTrue(e.getMessage().contains(
                    "Queue full. Consider increasing memory threshold or spooling to disk"));
            }

        }
    }

}
