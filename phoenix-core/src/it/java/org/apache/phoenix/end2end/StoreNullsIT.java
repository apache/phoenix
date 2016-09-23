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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests to demonstrate and verify the STORE_NULLS option on a table,
 * which allows explicitly storing null values (as opposed to using HBase Deletes) for nulls. This
 * functionality allows having row-level versioning (similar to how KEEP_DELETED_CELLS works), but
 * also allows permanently deleting a row.
 */
public class StoreNullsIT extends ParallelStatsDisabledIT {
    private static final Log LOG = LogFactory.getLog(StoreNullsIT.class);
    
    private String WITH_NULLS;
    private String WITHOUT_NULLS;
    private String IMMUTABLE_WITH_NULLS;
    private String IMMUTABLE_WITHOUT_NULLS;
    private Connection conn;
    private Statement stmt;

    @Before
    public void setUp() throws SQLException {
        WITH_NULLS = generateUniqueName();
        WITHOUT_NULLS = generateUniqueName();
        IMMUTABLE_WITH_NULLS = generateUniqueName();
        IMMUTABLE_WITHOUT_NULLS = generateUniqueName();
        conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(true);

        stmt = conn.createStatement();
        stmt.execute("CREATE TABLE " + WITH_NULLS + " (" +
                        "id SMALLINT NOT NULL PRIMARY KEY, " +
                        "name VARCHAR) " +
                "STORE_NULLS = true, VERSIONS = 1000, KEEP_DELETED_CELLS = false");
        stmt.execute("CREATE TABLE " + WITHOUT_NULLS + " (" +
                        "id SMALLINT NOT NULL PRIMARY KEY, " +
                        "name VARCHAR) " +
                "VERSIONS = 1000, KEEP_DELETED_CELLS = false");
        stmt.execute("CREATE TABLE " + IMMUTABLE_WITH_NULLS + " ("
                + "id SMALLINT NOT NULL PRIMARY KEY, name VARCHAR) "
                + "STORE_NULLS = true, VERSIONS = 1, KEEP_DELETED_CELLS = false, IMMUTABLE_ROWS=true");
        stmt.execute("CREATE TABLE " + IMMUTABLE_WITHOUT_NULLS + " ("
                + "id SMALLINT NOT NULL PRIMARY KEY, name VARCHAR) "
                + "VERSIONS = 1, KEEP_DELETED_CELLS = false, IMMUTABLE_ROWS=true");
    }

    @After
    public void tearDown() throws SQLException {
        stmt.close();
        conn.close();
    }

    @Test
    public void testStoringNulls() throws SQLException, InterruptedException, IOException {
        stmt.executeUpdate("UPSERT INTO " + IMMUTABLE_WITH_NULLS + " VALUES (1, 'v1')");
        stmt.executeUpdate("UPSERT INTO " + IMMUTABLE_WITHOUT_NULLS + " VALUES (1, 'v1')");
        stmt.executeUpdate("UPSERT INTO " + IMMUTABLE_WITH_NULLS + " VALUES (2, null)");
        stmt.executeUpdate("UPSERT INTO " + IMMUTABLE_WITHOUT_NULLS + " VALUES (2, null)");

        ensureNullsNotStored(IMMUTABLE_WITH_NULLS);
        ensureNullsNotStored(IMMUTABLE_WITHOUT_NULLS);
    }

    private void ensureNullsNotStored(String tableName) throws IOException {
        tableName = SchemaUtil.normalizeIdentifier(tableName);
        HTable htable = new HTable(getUtility().getConfiguration(), tableName);
        Scan s = new Scan();
        s.setRaw(true);
        ResultScanner scanner = htable.getScanner(s);
        // first row has a value for name
        Result rs = scanner.next();
        assertTrue(rs.containsColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("NAME")));
        assertTrue(rs.size() == 2);
        // 2nd row has not
        rs = scanner.next();
        assertFalse(rs.containsColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("NAME")));
        // and no delete marker either
        assertTrue(rs.size() == 1);
        assertNull(scanner.next());
        scanner.close();
        htable.close();
    }

    @Test
    public void testQueryingHistory() throws Exception {
        stmt.executeUpdate("UPSERT INTO " + WITH_NULLS + " VALUES (1, 'v1')");
        stmt.executeUpdate("UPSERT INTO " + WITHOUT_NULLS + " VALUES (1, 'v1')");

        Thread.sleep(10L);
        long afterFirstInsert = System.currentTimeMillis();
        Thread.sleep(10L);

        stmt.executeUpdate("UPSERT INTO " + WITH_NULLS + " VALUES (1, null)");
        stmt.executeUpdate("UPSERT INTO " + WITHOUT_NULLS + " VALUES (1, null)");
        Thread.sleep(10L);

        TestUtil.doMajorCompaction(conn, WITH_NULLS);
        TestUtil.doMajorCompaction(conn, WITHOUT_NULLS);

        Properties historicalProps = new Properties();
        historicalProps.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(afterFirstInsert));
        Connection historicalConn = DriverManager.getConnection(getUrl(), historicalProps);
        Statement historicalStmt = historicalConn.createStatement();

        ResultSet rs = historicalStmt.executeQuery(
            "SELECT name FROM " + WITH_NULLS + " WHERE id = 1");
        assertTrue(rs.next());
        assertEquals("v1", rs.getString(1));
        rs.close();

        // The single null wipes out all history for a field if STORE_NULLS is not enabled
        rs = historicalStmt.executeQuery("SELECT name FROM " + WITHOUT_NULLS + " WHERE id = 1");
        assertTrue(rs.next());
        assertNull(rs.getString(1));
        rs.close();

        historicalStmt.close();
        historicalConn.close();
    }

    // Row deletes should work in the same way regardless of what STORE_NULLS is set to
    @Test
    public void testDeletes() throws Exception {
        stmt.executeUpdate("UPSERT INTO " + WITH_NULLS + " VALUES (1, 'v1')");
        stmt.executeUpdate("UPSERT INTO " + WITHOUT_NULLS + " VALUES (1, 'v1')");

        Thread.sleep(10L);
        long afterFirstInsert = System.currentTimeMillis();
        Thread.sleep(10L);

        stmt.executeUpdate("DELETE FROM " + WITH_NULLS + " WHERE id = 1");
        stmt.executeUpdate("DELETE FROM " + WITHOUT_NULLS + " WHERE id = 1");
        Thread.sleep(10L);

        TestUtil.doMajorCompaction(conn, WITH_NULLS);
        TestUtil.doMajorCompaction(conn, WITHOUT_NULLS);

        Properties historicalProps = new Properties();
        historicalProps.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(afterFirstInsert));
        Connection historicalConn = DriverManager.getConnection(getUrl(), historicalProps);
        Statement historicalStmt = historicalConn.createStatement();

        // The row should be completely gone for both tables now

        ResultSet rs = historicalStmt.executeQuery(
            "SELECT name FROM " + WITH_NULLS + " WHERE id = 1");
        assertFalse(rs.next());
        rs.close();

        rs = historicalStmt.executeQuery("SELECT name FROM " + WITHOUT_NULLS + " WHERE id = 1");
        assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void testSetStoreNullsDefaultViaConfig() throws SQLException {
        Properties props = new Properties();
        props.setProperty(QueryServices.DEFAULT_STORE_NULLS_ATTRIB, "true");
        Connection storeNullsConn = DriverManager.getConnection(getUrl(), props);

        Statement stmt = storeNullsConn.createStatement();
        stmt.execute("CREATE TABLE with_nulls_default (" +
                "id smallint primary key," +
                "name varchar)");

        ResultSet rs = stmt.executeQuery("SELECT store_nulls FROM SYSTEM.CATALOG " +
                "WHERE table_name = 'WITH_NULLS_DEFAULT' AND store_nulls is not null");
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(1));
    }


}
