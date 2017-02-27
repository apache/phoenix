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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
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
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests to demonstrate and verify the STORE_NULLS option on a table,
 * which allows explicitly storing null values (as opposed to using HBase Deletes) for nulls. This
 * functionality allows having row-level versioning (similar to how KEEP_DELETED_CELLS works), but
 * also allows permanently deleting a row.
 */
@RunWith(Parameterized.class)
public class StoreNullsIT extends ParallelStatsDisabledIT {
    
    private final boolean mutable;
    private final boolean columnEncoded;
    private final boolean storeNulls;
    private final String ddlFormat;
    
    private String dataTableName;
    
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
    public static Collection<Boolean[]> data() {
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
        
        HTable htable = new HTable(getUtility().getConfiguration(), dataTableName);
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
        KeyValueColumnExpression colExpression = table.getImmutableStorageScheme() == ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS ? new SingleCellColumnExpression(nameColumn, "NAME", table.getEncodingScheme()) : new KeyValueColumnExpression(nameColumn);
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
            long afterFirstInsert = System.currentTimeMillis();
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
            long afterFirstInsert = System.currentTimeMillis();
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
        }
    }

}
