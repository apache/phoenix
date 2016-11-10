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
package org.apache.phoenix.iterate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.end2end.BaseUniqueNamesOwnClusterIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class RoundRobinResultIteratorWithStatsIT extends BaseUniqueNamesOwnClusterIT {
    
    private String tableName;
    
    @Before
    public void generateTableName() {
        tableName = generateUniqueName();
    }
    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        // Must update config before starting server
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(70000));
        props.put(QueryServices.QUEUE_SIZE_ATTRIB, Integer.toString(10000));
        props.put(QueryServices.EXPLAIN_CHUNK_COUNT_ATTRIB, Boolean.TRUE.toString());
        /*  
         * Don't force row key order. This causes RoundRobinResultIterator to be used if there was no order by specified
         * on the query.
         */
        props.put(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.toString(false));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    @Test
    public void testRoundRobinBehavior() throws Exception {
        int nRows = 30000;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + tableName + "(K VARCHAR PRIMARY KEY)");
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?)");
            for (int i = 1; i <= nRows; i++) {
                stmt.setString(1, i + "");
                stmt.executeUpdate();
                if ((i % 2000) == 0) {
                    conn.commit();
                }
            }
            conn.commit();
            conn.createStatement().execute("UPDATE STATISTICS " + tableName);
            PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
            MockParallelIteratorFactory parallelIteratorFactory = new MockParallelIteratorFactory();
            phxConn.setIteratorFactory(parallelIteratorFactory);
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
            StatementContext ctx = rs.unwrap(PhoenixResultSet.class).getContext();
            PTable table = ctx.getResolver().getTables().get(0).getTable();
            parallelIteratorFactory.setTable(table);
            PhoenixStatement pstmt = stmt.unwrap(PhoenixStatement.class);
            int numIterators = pstmt.getQueryPlan().getSplits().size();
            assertTrue(numIterators > 1);
            int numFetches = 2 * numIterators;
            List<String> iteratorOrder = new ArrayList<>(numFetches);
            for (int i = 1; i <= numFetches; i++) {
                rs.next();
                iteratorOrder.add(rs.getString(1));
            }
            /*
             * Because TableResultIterators are created in parallel in multiple threads, their relative order is not
             * deterministic. However, once the iterators are assigned to a RoundRobinResultIterator, the order in which
             * the next iterator is picked is deterministic - i1, i2, .. i7, i8, i1, i2, .. i7, i8, i1, i2, ..
             */
            for (int i = 0; i < numIterators; i++) {
                assertEquals(iteratorOrder.get(i), iteratorOrder.get(i + numIterators));
            }
        }
    }
    
}
