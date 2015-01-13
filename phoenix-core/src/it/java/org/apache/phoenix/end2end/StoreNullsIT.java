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

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests to demonstrate and verify the STORE_NULLS option on a table,
 * which allows explicitly storing null values (as opposed to using HBase Deletes) for nulls. This
 * functionality allows having row-level versioning (similar to how KEEP_DELETED_CELLS works), but
 * also allows permanently deleting a row.
 */
public class StoreNullsIT extends BaseHBaseManagedTimeIT {

    private static final Log LOG = LogFactory.getLog(StoreNullsIT.class);

    private Connection conn;
    private Statement stmt;

    @Before
    public void setUp() throws SQLException {
        conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(true);

        stmt = conn.createStatement();
        stmt.execute("CREATE TABLE with_nulls (" +
                        "id SMALLINT NOT NULL PRIMARY KEY, " +
                        "name VARCHAR) " +
                "STORE_NULLS = true, VERSIONS = 1000, KEEP_DELETED_CELLS = false");
        stmt.execute("CREATE TABLE without_nulls (" +
                        "id SMALLINT NOT NULL PRIMARY KEY, " +
                        "name VARCHAR) " +
                "VERSIONS = 1000, KEEP_DELETED_CELLS = false");
    }

    @After
    public void tearDown() throws SQLException {
        stmt.close();
        conn.close();
    }

    @Test
    public void testQueryingHistory() throws SQLException, InterruptedException, IOException {
        stmt.executeUpdate("UPSERT INTO with_nulls VALUES (1, 'v1')");
        stmt.executeUpdate("UPSERT INTO without_nulls VALUES (1, 'v1')");

        Thread.sleep(10L);
        long afterFirstInsert = System.currentTimeMillis();
        Thread.sleep(10L);

        stmt.executeUpdate("UPSERT INTO with_nulls VALUES (1, null)");
        stmt.executeUpdate("UPSERT INTO without_nulls VALUES (1, null)");
        Thread.sleep(10L);

        doMajorCompaction("with_nulls");
        doMajorCompaction("without_nulls");

        Properties historicalProps = new Properties();
        historicalProps.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(afterFirstInsert));
        Connection historicalConn = DriverManager.getConnection(getUrl(), historicalProps);
        Statement historicalStmt = historicalConn.createStatement();

        ResultSet rs = historicalStmt.executeQuery("SELECT name FROM with_nulls WHERE id = 1");
        assertTrue(rs.next());
        assertEquals("v1", rs.getString(1));
        rs.close();

        // The single null wipes out all history for a field if STORE_NULLS is not enabled
        rs = historicalStmt.executeQuery("SELECT name FROM without_nulls WHERE id = 1");
        assertTrue(rs.next());
        assertNull(rs.getString(1));
        rs.close();

        historicalStmt.close();
        historicalConn.close();
    }

    // Row deletes should work in the same way regardless of what STORE_NULLS is set to
    @Test
    public void testDeletes() throws SQLException, InterruptedException, IOException {
        stmt.executeUpdate("UPSERT INTO with_nulls VALUES (1, 'v1')");
        stmt.executeUpdate("UPSERT INTO without_nulls VALUES (1, 'v1')");

        Thread.sleep(10L);
        long afterFirstInsert = System.currentTimeMillis();
        Thread.sleep(10L);

        stmt.executeUpdate("DELETE FROM with_nulls WHERE id = 1");
        stmt.executeUpdate("DELETE FROM without_nulls WHERE id = 1");
        Thread.sleep(10L);

        doMajorCompaction("with_nulls");
        doMajorCompaction("without_nulls");

        Properties historicalProps = new Properties();
        historicalProps.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(afterFirstInsert));
        Connection historicalConn = DriverManager.getConnection(getUrl(), historicalProps);
        Statement historicalStmt = historicalConn.createStatement();

        // The row should be completely gone for both tables now

        ResultSet rs = historicalStmt.executeQuery("SELECT name FROM with_nulls WHERE id = 1");
        assertFalse(rs.next());
        rs.close();

        rs = historicalStmt.executeQuery("SELECT name FROM without_nulls WHERE id = 1");
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

    /**
     * Runs a major compaction, and then waits until the compaction is complete before returning.
     *
     * @param tableName name of the table to be compacted
     */
    private void doMajorCompaction(String tableName) throws IOException, InterruptedException {

        tableName = SchemaUtil.normalizeIdentifier(tableName);

        // We simply write a marker row, request a major compaction, and then wait until the marker
        // row is gone
        HTable htable = new HTable(getUtility().getConfiguration(), tableName);
        byte[] markerRowKey = Bytes.toBytes("TO_DELETE");


        Put put = new Put(markerRowKey);
        put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, HConstants.EMPTY_BYTE_ARRAY,
                HConstants.EMPTY_BYTE_ARRAY);
        htable.put(put);
        htable.delete(new Delete(markerRowKey));
        htable.close();

        HBaseAdmin hbaseAdmin = new HBaseAdmin(getUtility().getConfiguration());
        hbaseAdmin.flush(tableName);
        hbaseAdmin.majorCompact(tableName);
        hbaseAdmin.close();

        boolean compactionDone = false;
        while (!compactionDone) {
            Thread.sleep(2000L);
            htable = new HTable(getUtility().getConfiguration(), tableName);
            Scan scan = new Scan();
            scan.setStartRow(markerRowKey);
            scan.setStopRow(Bytes.add(markerRowKey, new byte[] { 0 }));
            scan.setRaw(true);

            ResultScanner scanner = htable.getScanner(scan);
            List<Result> results = Lists.newArrayList(scanner);
            LOG.info("Results: " + results);
            compactionDone = results.isEmpty();
            scanner.close();

            LOG.info("Compaction done: " + compactionDone);
        }

        htable.close();
    }


}
