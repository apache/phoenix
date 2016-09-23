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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

import com.google.common.collect.Sets;

public class RoundRobinResultIteratorIT extends ParallelStatsDisabledIT {

    private static final int NUM_SALT_BUCKETS = 4; 

    private static Connection getConnection() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        /*  
         * Don't force row key order. This causes RoundRobinResultIterator to be used if there was no order by specified
         * on the query.
         */
        props.setProperty(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.toString(false));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        return conn;
    }
    
    @Test
    public void testRoundRobinAfterTableSplit() throws Exception {
        String tableName = generateUniqueName();
        byte[] tableNameBytes = Bytes.toBytes(tableName);
        int numRows = setupTableForSplit(tableName);
        Connection conn = getConnection();
        ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
        int nRegions = services.getAllTableRegions(tableNameBytes).size();
        int nRegionsBeforeSplit = nRegions;
        HBaseAdmin admin = services.getAdmin();
        try {
            // Split is an async operation. So hoping 10 seconds is long enough time.
            // If the test tends to flap, then you might want to increase the wait time
            admin.split(tableName);
            CountDownLatch latch = new CountDownLatch(1);
            int nTries = 0;
            long waitTimeMillis = 2000;
            while (nRegions == nRegionsBeforeSplit && nTries < 10) {
                latch.await(waitTimeMillis, TimeUnit.MILLISECONDS);
                nRegions = services.getAllTableRegions(tableNameBytes).size();
                nTries++;
            }
            
            String query = "SELECT * FROM " + tableName;
            Statement stmt = conn.createStatement();
            stmt.setFetchSize(10); // this makes scanner caches to be replenished in parallel.
            ResultSet rs = stmt.executeQuery(query);
            int numRowsRead = 0;
            while (rs.next()) {
                numRowsRead++;
            }
            nRegions = services.getAllTableRegions(tableNameBytes).size();
            // Region cache has been updated, as there are more regions now
            assertNotEquals(nRegions, nRegionsBeforeSplit);
            assertEquals(numRows, numRowsRead);
        } finally {
            admin.close();
        }

    }

    @Test
    public void testSelectAllRowsWithDifferentFetchSizes_salted() throws Exception {
        testSelectAllRowsWithDifferentFetchSizes(true);
    }

    @Test
    public void testSelectAllRowsWithDifferentFetchSizes_unsalted() throws Exception {
        testSelectAllRowsWithDifferentFetchSizes(false);
    }

    private void testSelectAllRowsWithDifferentFetchSizes(boolean salted) throws Exception {
        String tableName = generateUniqueName();
        int numRows = 9;
        Set<String> expectedKeys = Collections.unmodifiableSet(createTableAndInsertRows(tableName, numRows, salted, false));
        Connection conn = getConnection();
        PreparedStatement stmt = conn.prepareStatement("SELECT K, V FROM " + tableName);
        tryWithFetchSize(new HashSet<>(expectedKeys), 1, stmt, 0);
        tryWithFetchSize(new HashSet<>(expectedKeys), 2, stmt, salted ? 2 : 5);
        tryWithFetchSize(new HashSet<>(expectedKeys), numRows - 1, stmt, salted ? 0 : 1);
        tryWithFetchSize(new HashSet<>(expectedKeys), numRows, stmt, salted ? 0 : 1);
        tryWithFetchSize(new HashSet<>(expectedKeys), numRows + 1, stmt, salted ? 0 : 1);
        tryWithFetchSize(new HashSet<>(expectedKeys), numRows + 2, stmt, 0);
    }

    @Test
    public void testSelectRowsWithFilterAndDifferentFetchSizes_unsalted() throws Exception {
        testSelectRowsWithFilterAndDifferentFetchSizes(false);
    }

    @Test
    public void testSelectRowsWithFilterAndDifferentFetchSizes_salted() throws Exception {
        testSelectRowsWithFilterAndDifferentFetchSizes(true);
    }

    private void testSelectRowsWithFilterAndDifferentFetchSizes(boolean salted) throws Exception {
        String tableName = generateUniqueName();
        int numRows = 6;
        Set<String> insertedKeys = createTableAndInsertRows(tableName, numRows, salted, false);
        Connection conn = getConnection();
        PreparedStatement stmt = conn.prepareStatement("SELECT K, V FROM " + tableName + " WHERE K = ?");
        stmt.setString(1, "key1"); // will return only 1 row
        int numRowsFiltered = 1;
        tryWithFetchSize(Sets.newHashSet("key1"), 1, stmt, 0);
        tryWithFetchSize(Sets.newHashSet("key1"), 2, stmt, salted ? 1 : 1);
        tryWithFetchSize(Sets.newHashSet("key1"), 3, stmt, 0);

        stmt = conn.prepareStatement("SELECT K, V FROM " + tableName + " WHERE K > ?");
        stmt.setString(1, "key2");
        insertedKeys.remove("key1");
        insertedKeys.remove("key2"); // query should return 4 rows after key2.
        numRowsFiltered = 4;
        tryWithFetchSize(new HashSet<>(insertedKeys), 1, stmt, 0);
        tryWithFetchSize(new HashSet<>(insertedKeys), 2, stmt, salted ? 1 : 2);
        tryWithFetchSize(new HashSet<>(insertedKeys), numRowsFiltered - 1, stmt, salted ? 0 : 1);
        tryWithFetchSize(new HashSet<>(insertedKeys), numRowsFiltered, stmt, salted ? 0 : 1);
        tryWithFetchSize(new HashSet<>(insertedKeys), numRowsFiltered + 1, stmt, salted ? 0 : 1);
        tryWithFetchSize(new HashSet<>(insertedKeys), numRowsFiltered + 2, stmt, 0);

        stmt = conn.prepareStatement("SELECT K, V FROM " + tableName + " WHERE K > ?");
        stmt.setString(1, "key6");
        insertedKeys.clear(); // query should return no rows;
        tryWithFetchSize(new HashSet<>(insertedKeys), 1, stmt, 0);
        tryWithFetchSize(new HashSet<>(insertedKeys), 2, stmt, 0);
        tryWithFetchSize(new HashSet<>(insertedKeys), numRows - 1, stmt, 0);
        tryWithFetchSize(new HashSet<>(insertedKeys), numRows, stmt, 0);
        tryWithFetchSize(new HashSet<>(insertedKeys), numRows + 1, stmt, 0);
    }

    private Set<String> createTableAndInsertRows(String tableName, int numRows, boolean salted, boolean addTableNameToKey) throws Exception {
        String ddl = "CREATE TABLE " + tableName + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)" + (salted ? "SALT_BUCKETS=" + NUM_SALT_BUCKETS : "");
        Connection conn = getConnection();
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + tableName + " VALUES (?, ?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        final Set<String> expectedKeys = new HashSet<>(numRows);
        for (int i = 1; i <= numRows; i++) {
            String key = (addTableNameToKey ? tableName : "") + ("key" + i);
            expectedKeys.add(key);
            stmt.setString(1, key);
            stmt.setString(2, "value" + i);
            stmt.executeUpdate();
        }
        conn.commit();
        return expectedKeys;
    }

    @Test
    public void testFetchSizesAndRVCExpression() throws Exception {
        String tableName = generateUniqueName();
        Set<String> insertedKeys = Collections.unmodifiableSet(createTableAndInsertRows(tableName, 4, false, false));
        Connection conn = getConnection();
        PreparedStatement stmt = conn.prepareStatement("SELECT K FROM " + tableName + " WHERE (K, V)  > (?, ?)");
        stmt.setString(1, "key0");
        stmt.setString(2, "value0");
        tryWithFetchSize(new HashSet<>(insertedKeys), 1, stmt, 0);
        tryWithFetchSize(new HashSet<>(insertedKeys), 2, stmt, 2);
        tryWithFetchSize(new HashSet<>(insertedKeys), 3, stmt, 1);
        tryWithFetchSize(new HashSet<>(insertedKeys), 4, stmt, 1);
    }

    private static void tryWithFetchSize(Set<String> expectedKeys, int fetchSize, PreparedStatement stmt, int numFetches) throws Exception {
        stmt.setFetchSize(fetchSize);
        ResultSet rs = stmt.executeQuery();
        int expectedNumRows = expectedKeys.size();
        int numRows = 0;
        while (rs.next()) {
            expectedKeys.remove(rs.getString(1));
            numRows ++;
        }
        assertEquals("Number of rows didn't match", expectedNumRows, numRows);
        assertTrue("Not all rows were returned for fetch size: " + fetchSize + " - " + expectedKeys, expectedKeys.size() == 0);
        assertRoundRobinBehavior(rs, stmt, numFetches);
    }

    private static int setupTableForSplit(String tableName) throws Exception {
        int batchSize = 25;
        int maxFileSize = 1024 * 10;
        int payLoadSize = 1024;
        String payload;
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < payLoadSize; i++) {
            buf.append('a');
        }
        payload = buf.toString();

        int MIN_CHAR = 'a';
        int MAX_CHAR = 'z';
        Connection conn = getConnection();
        conn.createStatement().execute("CREATE TABLE " + tableName + "("
                + "a VARCHAR PRIMARY KEY, b VARCHAR) " 
                + HTableDescriptor.MAX_FILESIZE + "=" + maxFileSize + ","
                + " SALT_BUCKETS = " + NUM_SALT_BUCKETS);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?)");
        int rowCount = 0;
        for (int c1 = MIN_CHAR; c1 <= MAX_CHAR; c1++) {
            for (int c2 = MIN_CHAR; c2 <= MAX_CHAR; c2++) {
                String pk = Character.toString((char)c1) + Character.toString((char)c2);
                stmt.setString(1, pk);
                stmt.setString(2, payload);
                stmt.execute();
                rowCount++;
                if (rowCount % batchSize == 0) {
                    conn.commit();
                }
            }
        }
        conn.commit();
        ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
        HBaseAdmin admin = services.getAdmin();
        try {
            admin.flush(tableName);
        } finally {
            admin.close();
        }
        conn.close();
        return rowCount;
    }

    @Test
    public void testUnionAllSelects() throws Exception {
        int insertedRowsA = 10;
        int insertedRowsB = 5;
        int insertedRowsC = 7;
        String baseTableName = generateUniqueName();
        String tableA = "TABLEA" + baseTableName;
        String tableB = "TABLEB" + baseTableName;
        String tableC = "TABLEC" + baseTableName;
        Set<String> keySetA = createTableAndInsertRows(tableA, insertedRowsA, true, true);
        Set<String> keySetB = createTableAndInsertRows(tableB, insertedRowsB, true, true);
        Set<String> keySetC = createTableAndInsertRows(tableC, insertedRowsC, false, true);
        String query = "SELECT K FROM " + tableA + " UNION ALL SELECT K FROM " + tableB + " UNION ALL SELECT K FROM " + tableC;
        Connection conn = getConnection();
        PreparedStatement stmt = conn.prepareStatement(query);
        stmt.setFetchSize(2); // force parallel fetch of scanner cache
        ResultSet rs = stmt.executeQuery();
        int rowsA = 0, rowsB = 0, rowsC = 0;
        while (rs.next()) {
            String key = rs.getString(1);
            if (key.startsWith("TABLEA")) {
                rowsA++;
            } else if (key.startsWith("TABLEB")) {
                rowsB++;
            } else if (key.startsWith("TABLEC")) {
                rowsC++;
            }
            keySetA.remove(key);
            keySetB.remove(key);
            keySetC.remove(key);
        }
        assertEquals("Not all rows of tableA were returned", 0, keySetA.size());
        assertEquals("Not all rows of tableB were returned", 0, keySetB.size());
        assertEquals("Not all rows of tableC were returned", 0, keySetC.size());
        assertEquals("Number of rows retrieved didn't match for tableA", insertedRowsA, rowsA);
        assertEquals("Number of rows retrieved didnt match for tableB", insertedRowsB, rowsB);
        assertEquals("Number of rows retrieved didn't match for tableC", insertedRowsC, rowsC);
    }
    
    @Test
    public void testBug2074() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = getConnection();
        try {
            conn.createStatement().execute("CREATE TABLE EVENTS" 
                    + "   (id VARCHAR(10) PRIMARY KEY, " 
                    + "    article VARCHAR(10), " 
                    + "    misc VARCHAR(10))");
            
            PreparedStatement upsertStmt = conn.prepareStatement(
                    "upsert into EVENTS(id, article, misc) " + "values (?, ?, ?)");
            upsertStmt.setString(1, "001");
            upsertStmt.setString(2, "A");
            upsertStmt.setString(3, "W");
            upsertStmt.execute();
            upsertStmt.setString(1, "002");
            upsertStmt.setString(2, "B");
            upsertStmt.setString(3, "X");
            upsertStmt.execute();
            upsertStmt.setString(1, "003");
            upsertStmt.setString(2, "C");
            upsertStmt.setString(3, "Y");
            upsertStmt.execute();
            upsertStmt.setString(1, "004");
            upsertStmt.setString(2, "D");
            upsertStmt.setString(3, "Z");
            upsertStmt.execute();
            conn.commit();

            conn.createStatement().execute("CREATE TABLE MAPPING" 
                    + "   (id VARCHAR(10) PRIMARY KEY, " 
                    + "    article VARCHAR(10), " 
                    + "    category VARCHAR(10))");
            
            upsertStmt = conn.prepareStatement(
                    "upsert into MAPPING(id, article, category) " + "values (?, ?, ?)");
            upsertStmt.setString(1, "002");
            upsertStmt.setString(2, "A");
            upsertStmt.setString(3, "X");
            upsertStmt.execute();
            upsertStmt.setString(1, "003");
            upsertStmt.setString(2, "B");
            upsertStmt.setString(3, "Y");
            upsertStmt.execute();
            upsertStmt.setString(1, "004");
            upsertStmt.setString(2, "C");
            upsertStmt.setString(3, "Z");
            upsertStmt.execute();
            upsertStmt.setString(1, "005");
            upsertStmt.setString(2, "E");
            upsertStmt.setString(3, "Z");
            upsertStmt.execute();
            upsertStmt.setString(1, "006");
            upsertStmt.setString(2, "C");
            upsertStmt.setString(3, "Z");
            upsertStmt.execute();
            upsertStmt.setString(1, "007");
            upsertStmt.setString(2, "C");
            upsertStmt.setString(3, "Z");
            upsertStmt.execute();
            conn.commit();

            // No leading part of PK
            String query = "select count(MAPPING.article) as cnt,MAPPING.category from EVENTS"
                    + " join MAPPING on MAPPING.article = EVENTS.article"
                    + " group by category order by cnt";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setFetchSize(3);
            ResultSet rs = statement.executeQuery();
            while(rs.next());
            
        } finally {
            conn.close();
        }
    }


    private static ResultIterator getResultIterator(ResultSet rs) throws SQLException {
        return rs.unwrap(PhoenixResultSet.class).getUnderlyingIterator();
    }

    private static void assertRoundRobinBehavior(ResultSet rs, Statement stmt, int numFetches) throws SQLException {
        ResultIterator itr = getResultIterator(rs);
        if (stmt.getFetchSize() > 1) {
            assertTrue(itr instanceof RoundRobinResultIterator);
            RoundRobinResultIterator roundRobinItr = (RoundRobinResultIterator)itr;
            assertEquals(numFetches, roundRobinItr.getNumberOfParallelFetches());
        }
    }
    
    @Test
    public void testIteratorsPickedInRoundRobinFashionForSaltedTable() throws Exception {
        try (Connection conn = getConnection()) {
            String testTable = "testIteratorsPickedInRoundRobinFashionForSaltedTable".toUpperCase();
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE " + testTable + "(K VARCHAR PRIMARY KEY) SALT_BUCKETS = 8");
            PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
            MockParallelIteratorFactory parallelIteratorFactory = new MockParallelIteratorFactory();
            phxConn.setIteratorFactory(parallelIteratorFactory);
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + testTable);
            StatementContext ctx = rs.unwrap(PhoenixResultSet.class).getContext();
            PTable table = ctx.getResolver().getTables().get(0).getTable();
            parallelIteratorFactory.setTable(table);
            PhoenixStatement pstmt = stmt.unwrap(PhoenixStatement.class);
            int numIterators = pstmt.getQueryPlan().getSplits().size();
            assertEquals(8, numIterators);
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
