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
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.cache.TenantCache;
import org.apache.phoenix.coprocessorclient.HashJoinCacheNotFoundException;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class HashJoinCacheIT extends BaseJoinIT {
    
    @Override
    protected String getTableName(Connection conn, String virtualName) throws Exception {
        String realName = super.getTableName(conn, virtualName);
        TestUtil.addCoprocessor(conn, SchemaUtil.normalizeFullTableName(realName), InvalidateHashCache.class);
        return realName;
    }

    @Test(expected = HashJoinCacheNotFoundException.class)
    public void testExpiredCache() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.MAX_SERVER_CACHE_TIME_TO_LIVE_MS_ATTRIB, "1");
        PreparedStatement statement = null;
        ResultSet rs = null;
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String tableName1 = getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME);
            String tableName2 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
            String query =
              "SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM "
                + tableName1
                + " supp RIGHT JOIN "
                + tableName2
                + " item ON item.\"supplier_id\" = supp.\"supplier_id\" ORDER BY \"item_id\"";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            rs.next();
            // should not reach here
            fail("HashJoinCacheNotFoundException was not thrown or incorrectly handled");
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (rs != null) {
                rs.close();
            }
        }
    }

    public static class InvalidateHashCache extends SimpleRegionObserver {
        public static Random rand= new Random();
        public static List<ImmutableBytesPtr> lastRemovedJoinIds=new ArrayList<ImmutableBytesPtr>();
        @Override
        public void preScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
              final Scan scan) {
            final HashJoinInfo joinInfo = HashJoinInfo.deserializeHashJoinFromScan(scan);
            if (joinInfo != null) {
                TenantCache cache = GlobalCache.getTenantCache(c.getEnvironment(), null);
                int count = joinInfo.getJoinIds().length;
                for (int i = 0; i < count; i++) {
                    ImmutableBytesPtr joinId = joinInfo.getJoinIds()[i];
                    if (!ByteUtil.contains(lastRemovedJoinIds,joinId)) {
                        lastRemovedJoinIds.add(joinId);
                        cache.removeServerCache(joinId);
                    }
                }
            }
        }
        
    }

    @Test(expected = HashJoinCacheNotFoundException.class)
    public void testExpiredCacheWithLeftJoin() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.MAX_SERVER_CACHE_TIME_TO_LIVE_MS_ATTRIB, "1");
        PreparedStatement statement = null;
        ResultSet rs = null;
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String tableName1 = getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME);
            String tableName2 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
            final String query =
              "SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM "
                + tableName1
                + " supp LEFT JOIN "
                + tableName2
                + " item ON item.\"supplier_id\" = supp.\"supplier_id\" ORDER BY \"item_id\"";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            rs.next();
            // should not reach here
            fail("HashJoinCacheNotFoundException was not thrown");
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (rs != null) {
                rs.close();
            }
        }
    }

    @Test(expected = HashJoinCacheNotFoundException.class)
    public void testExpiredCacheWithInnerJoin() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.MAX_SERVER_CACHE_TIME_TO_LIVE_MS_ATTRIB, "1");
        PreparedStatement statement = null;
        ResultSet rs = null;
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String tableName1 = getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME);
            String tableName2 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
            final String query =
              "SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM "
                + tableName1
                + " supp INNER JOIN "
                + tableName2
                + " item ON item.\"supplier_id\" = supp.\"supplier_id\" ORDER BY \"item_id\"";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            rs.next();
            // should not reach here
            fail("HashJoinCacheNotFoundException was not thrown as expected");
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (rs != null) {
                rs.close();
            }
        }
    }

}
