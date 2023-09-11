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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.SingleCellColumnExpression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests to demonstrate and verify the STORE_NULLS option on a table,
 * which allows explicitly storing null values (as opposed to using HBase Deletes) for nulls. This
 * functionality allows having row-level versioning (similar to how KEEP_DELETED_CELLS works), but
 * also allows permanently deleting a row.
 */
@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class StoreNullsIT extends ParallelStatsDisabledIT {
    
    private final boolean mutable;
    private final boolean columnEncoded;
    private final boolean storeNulls;
    private final String ddlFormat;
    
    private String dataTableName;

    // In this class we depend on the major compaction to remove every deleted row
    // so wo are overwriting the PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY to 0
    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(0));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    public StoreNullsIT(boolean mutable, boolean columnEncoded, boolean storeNulls) {
        this.mutable = mutable;
        this.columnEncoded = columnEncoded;
        this.storeNulls = storeNulls;
        
        StringBuilder sb = new StringBuilder("CREATE TABLE %s (id SMALLINT NOT NULL PRIMARY KEY, name VARCHAR) VERSIONS = 1000, KEEP_DELETED_CELLS = false ");
        if (!columnEncoded) {
            sb.append(",").append("COLUMN_ENCODED_BYTES=0");
        }
        if (!mutable) {
            sb.append(",").append("IMMUTABLE_ROWS=true");
            if (!columnEncoded) {
                sb.append(",IMMUTABLE_STORAGE_SCHEME="+PTableImpl.ImmutableStorageScheme.ONE_CELL_PER_COLUMN);
            }
        }
        if (storeNulls) {
            sb.append(",").append("STORE_NULLS=true");
        }
        this.ddlFormat = sb.toString();
    }
    
    @Parameters(name="StoreNullsIT_mutable={0}, columnEncoded={1}, storeNulls={2}") // name is used by failsafe as file name in reports
    public static synchronized Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] { 
                { false, false, false }, { false, false, true },
                { false, true, false }, { false, true, true },
                { true, false, false }, { true, false, true },
                { true, true, false }, { true, true, true }});
    }
    
    
    @Before
    public void setupTableNames() throws Exception {
        dataTableName = generateUniqueName();
    }

    @Test
    public void testStoringNullsForImmutableTables() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(true);
            stmt.execute(String.format(ddlFormat, dataTableName));
            stmt.executeUpdate("UPSERT INTO " + dataTableName + " VALUES (1, 'v1')");
            stmt.executeUpdate("UPSERT INTO " + dataTableName + " VALUES (2, null)");
            TestUtil.doMajorCompaction(conn, dataTableName);
            ensureNullsStoredCorrectly(conn);
        }
    }

    private void ensureNullsStoredCorrectly(Connection conn) throws Exception {
        ResultSet rs1 = conn.createStatement().executeQuery("SELECT NAME FROM "+dataTableName);
        rs1.next();
        assertEquals("v1", rs1.getString(1));
        rs1.next();
        assertNull(rs1.getString(1));
        rs1.next();
        Table htable =
                ConnectionFactory.createConnection(getUtility().getConfiguration()).getTable(
                    TableName.valueOf(dataTableName));
        Scan s = new Scan();
        s.setRaw(true);
        ResultScanner scanner = htable.getScanner(s);
        // first row has a value for name
        Result rs = scanner.next();
        PTable table = conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, dataTableName));
        PColumn nameColumn = table.getColumnForColumnName("NAME");
        byte[] qualifier = table.getImmutableStorageScheme()== ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS ? QueryConstants.SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES : nameColumn.getColumnQualifierBytes();
        assertTrue(rs.containsColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, qualifier));
        assertTrue(rs.size() == 2); // 2 because it also includes the empty key value column
        KeyValueColumnExpression colExpression =
                table.getImmutableStorageScheme() == ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS
                        ? new SingleCellColumnExpression(nameColumn, "NAME",
                                table.getEncodingScheme(), table.getImmutableStorageScheme())
                        : new KeyValueColumnExpression(nameColumn);
        ImmutableBytesPtr ptr = new ImmutableBytesPtr();
        colExpression.evaluate(new ResultTuple(rs), ptr);
        assertEquals(new ImmutableBytesPtr(PVarchar.INSTANCE.toBytes("v1")), ptr);
        rs = scanner.next();
        
        if ( !mutable && !columnEncoded // we don't issue a put with empty value for immutable tables with cols stored per key value
                || (mutable && !storeNulls)) { // for this case we use a delete to represent the null
            assertFalse(rs.containsColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, qualifier));
            assertEquals(1, rs.size());
        }
        else { 
            assertTrue(rs.containsColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, qualifier));
            assertEquals(2, rs.size()); 
        }
        // assert null stored correctly 
        ptr = new ImmutableBytesPtr();
        if (colExpression.evaluate(new ResultTuple(rs), ptr)) {
            assertEquals(new ImmutableBytesPtr(ByteUtil.EMPTY_BYTE_ARRAY), ptr);
        }
        assertNull(scanner.next());
        scanner.close();
        htable.close();
    }

    @Test
    public void testQueryingHistory() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(true);
            stmt.execute(String.format(ddlFormat, dataTableName));
            stmt.executeUpdate("UPSERT INTO " + dataTableName + " VALUES (1, 'v1')");
            Thread.sleep(10L);
            long afterFirstInsert = EnvironmentEdgeManager.currentTimeMillis();
            Thread.sleep(10L);
            
            stmt.executeUpdate("UPSERT INTO " + dataTableName + " VALUES (1, null)");
            Thread.sleep(10L);
            
            TestUtil.doMajorCompaction(conn, dataTableName);
            
            Properties historicalProps = new Properties();
            historicalProps.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(afterFirstInsert));
            Connection historicalConn = DriverManager.getConnection(getUrl(), historicalProps);
            Statement historicalStmt = historicalConn.createStatement();
            ResultSet rs = historicalStmt.executeQuery( "SELECT name FROM " + dataTableName + " WHERE id = 1");
            
            if (storeNulls || !mutable) { // store nulls is set to true if the table is immutable
                assertTrue(rs.next());
                assertEquals("v1", rs.getString(1));
                rs.close();
            } 
            else {
                // The single null wipes out all history for a field if STORE_NULLS is not enabled
                assertTrue(rs.next());
                assertNull(rs.getString(1));
            }
            
            rs.close();
            historicalStmt.close();
            conn.close();
            historicalConn.close();
        }

    }

    // Row deletes should work in the same way regardless of what STORE_NULLS is set to
    @Test
    public void testDeletes() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(true);
            stmt.execute(String.format(ddlFormat, dataTableName));
            stmt.executeUpdate("UPSERT INTO " + dataTableName + " VALUES (1, 'v1')");
    
            Thread.sleep(10L);
            long afterFirstInsert = EnvironmentEdgeManager.currentTimeMillis();
            Thread.sleep(10L);
    
            stmt.executeUpdate("DELETE FROM " + dataTableName + " WHERE id = 1");
            Thread.sleep(10L);
            TestUtil.doMajorCompaction(conn, dataTableName);
            Properties historicalProps = new Properties();
            historicalProps.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                    Long.toString(afterFirstInsert));
            Connection historicalConn = DriverManager.getConnection(getUrl(), historicalProps);
            Statement historicalStmt = historicalConn.createStatement();
    
            // The row should be completely gone for both tables now
    
            ResultSet rs = historicalStmt.executeQuery(
                "SELECT name FROM " + dataTableName + " WHERE id = 1");
            assertFalse(rs.next());
            rs.close();
    
            rs = historicalStmt.executeQuery("SELECT name FROM " + dataTableName + " WHERE id = 1");
            assertFalse(rs.next());
            rs.close();
            conn.close();
            historicalStmt.close();
            historicalConn.close();
        }
    }

    
    private static long getRowCount(Connection conn, String tableName) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ count(*) FROM " + tableName);
        assertTrue(rs.next());
        return rs.getLong(1);
    }

    @Test
    public void testSetIndexedColumnToNullTwiceWithStoreNulls() throws Exception {
        if (!mutable) {
            return;
        }
        
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);     
        conn.createStatement().execute("CREATE TABLE " + tableName + "(k1 CHAR(2) NOT NULL, k2 CHAR(2) NOT NULL, ts TIMESTAMP, V VARCHAR, V2 VARCHAR, "
                + "CONSTRAINT pk PRIMARY KEY (k1,k2)) STORE_NULLS=" + storeNulls + (columnEncoded ? "" : ",COLUMN_ENCODED_BYTES=0"));
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(k2,k1,ts) INCLUDE (V, v2)");
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, '0')");
        stmt.setTimestamp(1, new Timestamp(1000L));
        stmt.executeUpdate();
        conn.commit();
        
        Timestamp expectedTimestamp;
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, null)");
        expectedTimestamp = null;
        stmt.setTimestamp(1, expectedTimestamp);
        stmt.executeUpdate();
        conn.commit();
        
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, null)");
        expectedTimestamp = null;
        stmt.setTimestamp(1, expectedTimestamp);
        stmt.executeUpdate();
        conn.commit();

        TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(tableName)));
        TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(indexName)));

        long count1 = getRowCount(conn, tableName);
        long count2 = getRowCount(conn, indexName);
        assertEquals("Table should have 1 row", 1, count1);
        assertEquals("Index should have 1 row", 1, count2);
        
        ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ ts,v FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(expectedTimestamp, rs.getTimestamp(1));
        assertEquals(null, rs.getString(2));
        assertFalse(rs.next());
        
        rs = conn.createStatement().executeQuery("SELECT \"0:TS\", \"0:V\" FROM " + indexName);
        assertTrue(rs.next());
        assertEquals(expectedTimestamp, rs.getTimestamp(1));
        assertEquals(null, rs.getString(2));
        assertFalse(rs.next());
        conn.close();
    }

}
