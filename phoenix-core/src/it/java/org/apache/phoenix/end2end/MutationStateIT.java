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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Properties;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.junit.Test;

public class MutationStateIT extends ParallelStatsDisabledIT {

    private static final String DDL =
            " (ORGANIZATION_ID CHAR(15) NOT NULL, SCORE DOUBLE, "
            + "ENTITY_ID CHAR(15) NOT NULL, TAGS VARCHAR, CONSTRAINT PAGE_SNAPSHOT_PK "
            + "PRIMARY KEY (ORGANIZATION_ID, ENTITY_ID DESC)) MULTI_TENANT=TRUE";

    private void upsertRows(PhoenixConnection conn, String fullTableName) throws SQLException {
        PreparedStatement stmt =
                conn.prepareStatement("upsert into " + fullTableName
                        + " (organization_id, entity_id, score) values (?,?,?)");
        for (int i = 0; i < 10000; i++) {
            stmt.setString(1, "AAAA" + i);
            stmt.setString(2, "BBBB" + i);
            stmt.setInt(3, 1);
            stmt.execute();
        }
    }

    @Test
    public void testDeleteMaxMutationSize() throws SQLException {
        String tableName = generateUniqueName();
        int NUMBER_OF_ROWS = 20;
        String ddl = "CREATE TABLE " + tableName + " (V BIGINT PRIMARY KEY, K BIGINT)";
        PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl);

        for(int i = 0; i < NUMBER_OF_ROWS; i++) {
            conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " VALUES (" + i + ", "+ i + ")");
            conn.commit();
        }

        Properties props = new Properties();
        props.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB,
                String.valueOf(NUMBER_OF_ROWS / 2));
        PhoenixConnection connection =
                (PhoenixConnection) DriverManager.getConnection(getUrl(), props);
        connection.setAutoCommit(false);

        try {
            for(int i = 0; i < NUMBER_OF_ROWS; i++) {
                connection.createStatement().execute(
                        "DELETE FROM " + tableName + " WHERE K = " + i );
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains(
                    SQLExceptionCode.MAX_MUTATION_SIZE_EXCEEDED.getMessage()));
        }

        props.setProperty(QueryServices.MAX_MUTATION_SIZE_BYTES_ATTRIB, "10");
        props.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB, "10000");
        connection = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);
        connection.setAutoCommit(false);

        try {
            connection.createStatement().execute("DELETE FROM " + tableName );
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains(
                    SQLExceptionCode.MAX_MUTATION_SIZE_BYTES_EXCEEDED.getMessage()));
        }
    }

    @Test
    public void testUpsertMaxMutationSize() throws Exception {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB, "3");
        connectionProperties.setProperty(QueryServices.MAX_MUTATION_SIZE_BYTES_ATTRIB, "1000000");
        PhoenixConnection connection =
                (PhoenixConnection) DriverManager.getConnection(getUrl(), connectionProperties);
        String fullTableName = generateUniqueName();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                "CREATE TABLE " + fullTableName + DDL);
        }
        try {
            upsertRows(connection, fullTableName);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.MAX_MUTATION_SIZE_EXCEEDED.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getMessage().contains(
                    SQLExceptionCode.MAX_MUTATION_SIZE_EXCEEDED.getMessage()));
        }

        // set the max mutation size (bytes) to a low value
        connectionProperties.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB, "1000");
        connectionProperties.setProperty(QueryServices.MAX_MUTATION_SIZE_BYTES_ATTRIB, "4");
        connection =
                (PhoenixConnection) DriverManager.getConnection(getUrl(), connectionProperties);
        try {
            upsertRows(connection, fullTableName);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.MAX_MUTATION_SIZE_BYTES_EXCEEDED.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getMessage().contains(
                    SQLExceptionCode.MAX_MUTATION_SIZE_BYTES_EXCEEDED.getMessage()));
        }
    }

    @Test
    public void testMutationEstimatedSize() throws Exception {
        PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl());
        conn.setAutoCommit(false);
        String fullTableName = generateUniqueName();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(
                "CREATE TABLE " + fullTableName + DDL);
        }

        // upserting rows should increase the mutation state size
        MutationState state = conn.unwrap(PhoenixConnection.class).getMutationState();
        long prevEstimatedSize = state.getEstimatedSize();
        upsertRows(conn, fullTableName);
        assertTrue("Mutation state size should have increased",
            state.getEstimatedSize() > prevEstimatedSize);
        
        
        // after commit or rollback the size should be zero
        conn.commit();
        assertEquals("Mutation state size should be zero after commit", 0,
            state.getEstimatedSize());
        upsertRows(conn, fullTableName);
        conn.rollback();
        assertEquals("Mutation state size should be zero after rollback", 0,
            state.getEstimatedSize());

        // upsert one row
        PreparedStatement stmt =
                conn.prepareStatement("upsert into " + fullTableName
                        + " (organization_id, entity_id, score) values (?,?,?)");
        stmt.setString(1, "ZZZZ");
        stmt.setString(2, "YYYY");
        stmt.setInt(3, 1);
        stmt.execute();
        assertTrue("Mutation state size should be greater than zero ", state.getEstimatedSize()>0);

        prevEstimatedSize = state.getEstimatedSize();
        // upserting the same row twice should not increase the size
        stmt.setString(1, "ZZZZ");
        stmt.setString(2, "YYYY");
        stmt.setInt(3, 1);
        stmt.execute();
        assertEquals(
            "Mutation state size should only increase 4 bytes (size of the new statement index)",
            prevEstimatedSize + 4, state.getEstimatedSize());
        
        prevEstimatedSize = state.getEstimatedSize();
        // changing the value of one column of a row to a larger value should increase the estimated size 
        stmt =
                conn.prepareStatement("upsert into " + fullTableName
                        + " (organization_id, entity_id, score, tags) values (?,?,?,?)");
        stmt.setString(1, "ZZZZ");
        stmt.setString(2, "YYYY");
        stmt.setInt(3, 1);
        stmt.setString(4, "random text string random text string random text string");
        stmt.execute();
        assertTrue("Mutation state size should increase", prevEstimatedSize+4 < state.getEstimatedSize());
        
        prevEstimatedSize = state.getEstimatedSize();
        // changing the value of one column of a row to a smaller value should decrease the estimated size 
        stmt =
                conn.prepareStatement("upsert into " + fullTableName
                        + " (organization_id, entity_id, score, tags) values (?,?,?,?)");
        stmt.setString(1, "ZZZZ");
        stmt.setString(2, "YYYY");
        stmt.setInt(3, 1);
        stmt.setString(4, "");
        stmt.execute();
        assertTrue("Mutation state size should decrease", prevEstimatedSize+4 > state.getEstimatedSize());
    }

    @Test
    public void testSplitMutationsIntoSameGroupForSingleRow() throws Exception {
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        Properties props = new Properties();
        props.put("phoenix.mutate.batchSize", "2");
        try (PhoenixConnection conn = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
            conn.setAutoCommit(false);
            conn.createStatement().executeUpdate(
                    "CREATE TABLE "  + tableName + " ("
                            + "A VARCHAR NOT NULL PRIMARY KEY,"
                            + "B VARCHAR,"
                            + "C VARCHAR,"
                            + "D VARCHAR) COLUMN_ENCODED_BYTES = 0");
            conn.createStatement().executeUpdate("CREATE INDEX " + indexName + " on "  + tableName + " (C) INCLUDE(D)");

            conn.createStatement().executeUpdate("UPSERT INTO "  + tableName + "(A,B,C,D) VALUES ('A2','B2','C2','D2')");
            conn.createStatement().executeUpdate("UPSERT INTO "  + tableName + "(A,B,C,D) VALUES ('A3','B3', 'C3', null)");
            conn.commit();

            Table htable = conn.getQueryServices().getTable(Bytes.toBytes(tableName));
            Scan scan = new Scan();
            scan.setRaw(true);
            Iterator<Result> scannerIter = htable.getScanner(scan).iterator();
            while (scannerIter.hasNext()) {
                long ts = -1;
                Result r = scannerIter.next();
                for (Cell cell : r.listCells()) {
                    if (ts == -1) {
                        ts = cell.getTimestamp();
                    } else {
                        assertEquals("(" + cell.toString() + ") has different ts", ts, cell.getTimestamp());
                    }
                }
            }
            htable.close();
        }
    }
}
