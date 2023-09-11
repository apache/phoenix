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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.Repeat;
import org.apache.phoenix.util.RunUntilFailure;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@Category(ParallelStatsDisabledTest.class)
@RunWith(RunUntilFailure.class)
public class MutationStateIT extends ParallelStatsDisabledIT {

    private static final String DDL =
            " (ORGANIZATION_ID CHAR(15) NOT NULL, SCORE DOUBLE, "
            + "ENTITY_ID CHAR(15) NOT NULL, TAGS VARCHAR, CONSTRAINT PAGE_SNAPSHOT_PK "
            + "PRIMARY KEY (ORGANIZATION_ID, ENTITY_ID DESC)) MULTI_TENANT=TRUE";

    private static final Random RAND = new Random(5);

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

    public static String randString(int length) {
        return new BigInteger(164, RAND).toString().substring(0, length);
    }

    private static void mutateRandomly(final String upsertStmt, final String fullTableName,
            final int nThreads, final int nRows, final int nIndexValues, final int batchSize,
            final CountDownLatch doneSignal) {
        Runnable[] runnables = new Runnable[nThreads];
        for (int i = 0; i < nThreads; i++) {
            runnables[i] = new Runnable() {

                @Override
                public void run() {
                    try {
                        Connection conn = DriverManager.getConnection(getUrl());
                        for (int i = 0; i < nRows; i++) {
                            PreparedStatement statement = conn.prepareStatement(upsertStmt);
                            int index = 0;
                            statement.setString(++index, randString(15));
                            statement.setString(++index, randString(15));
                            statement.setString(++index, randString(15));
                            statement.setString(++index, randString(1));
                            statement.setString(++index, randString(15));
                            statement.setString(++index, randString(15));
                            statement.setTimestamp(++index,
                                new Timestamp(System.currentTimeMillis()));
                            statement.setTimestamp(++index,
                                new Timestamp(System.currentTimeMillis()));
                            statement.setString(++index, randString(1));
                            statement.setString(++index, randString(1));
                            statement.setBoolean(++index, false);
                            statement.setString(++index, randString(1));
                            statement.setString(++index, randString(1));
                            statement.setString(++index, randString(15));
                            statement.setString(++index, randString(15));
                            statement.setString(++index, randString(15));
                            statement.setInt(++index, RAND.nextInt());
                            statement.execute();
                            if ((i % batchSize) == 0) {
                                conn.commit();
                            }
                        }
                        conn.commit();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    } finally {
                        doneSignal.countDown();
                    }
                }

            };
        }
        for (int i = 0; i < nThreads; i++) {
            Thread t = new Thread(runnables[i]);
            t.start();
        }
    }

    @Test
    @Repeat(10)
    public void testOnlyIndexTableWriteFromClientSide()
            throws SQLException, InterruptedException, IOException {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName1 = generateUniqueName();
        String indexName2 = generateUniqueName();
        String indexName3 = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName1 = SchemaUtil.getTableName(schemaName, indexName1);
        String CREATE_DATA_TABLE =
                "CREATE TABLE IF NOT EXISTS " + fullTableName + " ( \n"
                        + "    USER1_ID CHAR(15) NOT NULL,\n"
                        + "    ELEMENT1_ID CHAR(15) NOT NULL,\n"
                        + "    ELEMENT_ID CHAR(15) NOT NULL,\n"
                        + "    ELEMENT_TYPE VARCHAR(1) NOT NULL,\n"
                        + "    TYPE_ID CHAR(15) NOT NULL,\n"
                        + "    USER_ID CHAR(15) NOT NULL,\n"
                        + "    ELEMENT4_TIME TIMESTAMP,\n"
                        + "    ELEMENT_UPDATE TIMESTAMP,\n"
                        + "    ELEMENT_SCORE DOUBLE,\n"
                        + "    ELEMENT2_TYPE VARCHAR(1),\n"
                        + "    ELEMENT1_TYPE VARCHAR(1),\n"
                        + "    ELEMENT1_IS_SYS_GEN BOOLEAN,\n"
                        + "    ELEMENT1_STATUS VARCHAR(1),\n"
                        + "    ELEMENT1_VISIBILITY VARCHAR(1),\n"
                        + "    ELEMENT3_ID CHAR(15),\n"
                        + "    ELEMENT4_BY CHAR(15),\n"
                        + "    BEST_ELEMENT_ID CHAR(15),\n"
                        + "    ELEMENT_COUNT INTEGER,\n"
                        + "    CONSTRAINT PK PRIMARY KEY\n"
                        + "    (\n" + "     USER1_ID,\n"
                        + "     ELEMENT1_ID,\n"
                        + "     ELEMENT_ID,\n"
                        + "     ELEMENT_TYPE,\n"
                        + "     TYPE_ID,\n"
                        + "     USER_ID\n" + " )\n"
                        + " ) VERSIONS=1,MULTI_TENANT=TRUE,TTL=31536000\n";

        String CREATE_INDEX_1 =
                "CREATE INDEX IF NOT EXISTS " + indexName1 + " \n"
                        + "     ON " + fullTableName + " (\n"
                    + "     TYPE_ID,\n"
                    + "     ELEMENT_ID,\n"
                    + "     ELEMENT_TYPE,\n"
                    + "     USER_ID,\n"
                    + "     ELEMENT4_TIME DESC,\n"
                    + "     ELEMENT1_ID DESC\n"
                    + "     ) INCLUDE (\n"
                    + "     ELEMENT2_TYPE,\n"
                    + "     ELEMENT1_TYPE,\n"
                    + "     ELEMENT1_IS_SYS_GEN,\n"
                    + "     ELEMENT1_STATUS,\n"
                    + "     ELEMENT1_VISIBILITY,\n"
                    + "     ELEMENT3_ID,\n"
                    + "     ELEMENT4_BY,\n"
                    + "     BEST_ELEMENT_ID,\n"
                    + "     ELEMENT_COUNT\n"
                    + "     )\n";

        String CREATE_INDEX_2 =
                " CREATE INDEX IF NOT EXISTS " + indexName2  + "\n"
                        + "     ON " + fullTableName + " (\n"
                    + "     TYPE_ID,\n"
                    + "     ELEMENT_ID,\n"
                    + "     ELEMENT_TYPE,\n"
                    + "     USER_ID,\n"
                    + "     ELEMENT_UPDATE DESC,\n"
                    + "     ELEMENT1_ID DESC\n"
                    + "     ) INCLUDE (\n"
                    + "     ELEMENT2_TYPE,\n"
                    + "     ELEMENT1_TYPE,\n"
                    + "     ELEMENT1_IS_SYS_GEN,\n"
                    + "     ELEMENT1_STATUS,\n"
                    + "     ELEMENT1_VISIBILITY,\n"
                    + "     ELEMENT3_ID,\n"
                    + "     ELEMENT4_BY,\n"
                    + "     BEST_ELEMENT_ID,\n"
                    + "     ELEMENT_COUNT\n"
                    + "     )\n";

        String CREATE_INDEX_3 =
                "CREATE INDEX IF NOT EXISTS " + indexName3  + "\n"
                        + "     ON " + fullTableName + " (\n"
                    + "     TYPE_ID,\n"
                    + "     ELEMENT_ID,\n"
                    + "     ELEMENT_TYPE,\n"
                    + "     USER_ID,\n"
                    + "     ELEMENT_SCORE DESC,\n"
                    + "     ELEMENT1_ID DESC\n"
                    + "     ) INCLUDE (\n"
                    + "     ELEMENT2_TYPE,\n"
                    + "     ELEMENT1_TYPE,\n"
                    + "     ELEMENT1_IS_SYS_GEN,\n"
                    + "     ELEMENT1_STATUS,\n"
                    + "     ELEMENT1_VISIBILITY,\n"
                    + "     ELEMENT3_ID,\n"
                    + "     ELEMENT4_BY,\n"
                    + "     BEST_ELEMENT_ID,\n"
                    + "     ELEMENT_COUNT\n"
                    + "     )\n";

        String UPSERT_INTO_DATA_TABLE =
                "UPSERT INTO " + fullTableName + "\n"
                        + "(\n" + "    USER1_ID,\n"
                        + "    ELEMENT1_ID,\n"
                        + "    ELEMENT_ID,\n"
                        + "    ELEMENT_TYPE,\n"
                        + "    TYPE_ID,\n"
                        + "    USER_ID,\n"
                        + "    ELEMENT4_TIME,\n"
                        + "    ELEMENT_UPDATE,\n"
                        + "    ELEMENT2_TYPE,\n"
                        + "    ELEMENT1_TYPE,\n"
                        + "    ELEMENT1_IS_SYS_GEN,\n"
                        + "    ELEMENT1_STATUS,\n"
                        + "    ELEMENT1_VISIBILITY,\n"
                        + "    ELEMENT3_ID,\n"
                        + "    ELEMENT4_BY,\n"
                        + "    BEST_ELEMENT_ID,\n"
                        + "    ELEMENT_COUNT\n" + ")"
                      + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        int nThreads = 1;
        int nRows = 5000;
        int nIndexValues = 4000;
        int batchSize = 200;
        final CountDownLatch doneSignal = new CountDownLatch(nThreads);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            try {
                conn.createStatement().execute(CREATE_DATA_TABLE);
                conn.createStatement().execute(CREATE_INDEX_1);
                conn.createStatement().execute(CREATE_INDEX_2);
                conn.createStatement().execute(CREATE_INDEX_3);
                conn.commit();
                mutateRandomly(UPSERT_INTO_DATA_TABLE, fullTableName, nThreads, nRows, nIndexValues,
                    batchSize, doneSignal);
                Thread.sleep(200);
                unassignRegionAsync(fullIndexName1);
                assertTrue("Ran out of time", doneSignal.await(120, TimeUnit.SECONDS));
            } finally {

            }
            long dataTableRows = TestUtil.getRowCount(conn, fullTableName);
            ResultSet rs =
                    conn.getMetaData().getTables(null, StringUtil.escapeLike(schemaName), null,
                        new String[] { PTableType.INDEX.toString() });
            while (rs.next()) {
                String indexState = rs.getString("INDEX_STATE");
                String indexName = rs.getString(3);
                long rowCountIndex =
                        TestUtil.getRowCount(conn, SchemaUtil.getTableName(schemaName, indexName));
                if (indexState.equals(PIndexState.ACTIVE.name())) {
                    assertTrue(dataTableRows == rowCountIndex);
                } else {
                    assertTrue(dataTableRows > rowCountIndex);
                }
            }

        } catch (InterruptedException e) {
            throw e;
        } catch (IOException e) {
            throw e;
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
            assertTrue(e.getMessage().contains(
                    connectionProperties.getProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB)));
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
            assertTrue(e.getMessage().contains(connectionProperties.getProperty
                    (QueryServices.MAX_MUTATION_SIZE_BYTES_ATTRIB)));
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

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void testDDLwithPendingMutations() throws Exception {
        String tableName = generateUniqueName();
        ensureTableCreated(getUrl(), tableName, TestUtil.PTSDB_NAME, null, null, null);
        Properties props = new Properties();
        props.setProperty(QueryServices.PENDING_MUTATIONS_DDL_THROW_ATTRIB, Boolean.toString(true));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            // setting auto commit to false
            conn.setAutoCommit(false);

            // Run upsert queries but do not commit
            PreparedStatement stmt =
                    conn.prepareStatement("UPSERT INTO " + tableName
                            + " (inst,host,\"DATE\") VALUES(?,'b',CURRENT_DATE())");
            stmt.setString(1, "a");
            stmt.execute();
            // Create a ddl statement
            String tableName2 = generateUniqueName();
            String ddl = "CREATE TABLE " + tableName2 + " (V BIGINT PRIMARY KEY, K BIGINT)";
            exceptionRule.expect(SQLException.class);
            exceptionRule.expectMessage(
                SQLExceptionCode.CANNOT_PERFORM_DDL_WITH_PENDING_MUTATIONS.getMessage());
            conn.createStatement().execute(ddl);
        }
    }

    @Test
    public void testNoPendingMutationsOnDDL() throws Exception {
        Properties props = new Properties();
        props.setProperty(QueryServices.PENDING_MUTATIONS_DDL_THROW_ATTRIB, Boolean.toString(true));
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String ddl =
                    "create table " + tableName + " ( id1 UNSIGNED_INT not null primary key,"
                            + "appId1 VARCHAR)";
            conn.createStatement().execute(ddl);
            // ensure table got created
            Admin admin = driver.getConnectionQueryServices(getUrl(), props).getAdmin();
            assertNotNull(admin.getDescriptor(TableName.valueOf(tableName)));
            assertNotNull(PhoenixRuntime.getTableNoCache(conn, tableName));
        }
    }

    @Test
    public void testUpsertMaxColumnAllowanceForSingleCellArrayWithOffsets() throws Exception {
        testUpsertColumnExceedsMaxAllowanceSize("SINGLE_CELL_ARRAY_WITH_OFFSETS");
    }

    @Test
    public void testUpsertMaxColumnAllowanceForOneCellPerColumn() throws Exception {
        testUpsertColumnExceedsMaxAllowanceSize("ONE_CELL_PER_COLUMN");
    }

    public void testUpsertColumnExceedsMaxAllowanceSize(String storageScheme) throws Exception {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty(QueryServices.HBASE_CLIENT_KEYVALUE_MAXSIZE, "20");
        try (PhoenixConnection connection =
                     (PhoenixConnection) DriverManager.getConnection(getUrl(), connectionProperties)) {
            String fullTableName = generateUniqueName();
            String pk1Name = generateUniqueName();
            String pk2Name = generateUniqueName();
            String ddl = "CREATE IMMUTABLE TABLE " + fullTableName +
                    " (" +  pk1Name + " VARCHAR(15) NOT NULL, " + pk2Name + " VARCHAR(15) NOT NULL, " +
                    "PAYLOAD1 VARCHAR, PAYLOAD2 VARCHAR,PAYLOAD3 VARCHAR " +
                    "CONSTRAINT PK PRIMARY KEY (" + pk1Name + "," + pk2Name+ ")) " +
                    "IMMUTABLE_STORAGE_SCHEME =" + storageScheme;
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(ddl);
            }
            String sql = "UPSERT INTO " + fullTableName +
                    " ("+ pk1Name + ","+ pk2Name + ",PAYLOAD1,PAYLOAD2,PAYLOAD2) VALUES (?,?,?,?,?)";
            String pk1Value = generateUniqueName();
            String pk2Value = generateUniqueName();
            String payload1Value = generateUniqueName();
            String payload3Value = generateUniqueName();

            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setString(1, pk1Value);
                preparedStatement.setString(2, pk2Value);
                preparedStatement.setString(3, payload1Value);
                preparedStatement.setString(4, "1234567890");
                preparedStatement.setString(5, payload3Value);
                preparedStatement.execute();

                try {
                    preparedStatement.setString(1, pk1Value);
                    preparedStatement.setString(2, pk2Value);
                    preparedStatement.setString(3, payload1Value);
                    preparedStatement.setString(4, "12345678901234567890");
                    preparedStatement.setString(5, payload3Value);
                    preparedStatement.execute();
                    if (storageScheme.equals("ONE_CELL_PER_COLUMN")) {
                        fail();
                    }
                } catch (SQLException e) {
                    if (!storageScheme.equals("ONE_CELL_PER_COLUMN")) {
                        fail();
                    } else {
                        assertEquals(SQLExceptionCode.MAX_HBASE_CLIENT_KEYVALUE_MAXSIZE_EXCEEDED.getErrorCode(),
                                e.getErrorCode());
                        assertTrue(e.getMessage().contains(
                                SQLExceptionCode.MAX_HBASE_CLIENT_KEYVALUE_MAXSIZE_EXCEEDED.getMessage()));
                        assertTrue(e.getMessage().contains(
                                connectionProperties.getProperty(QueryServices.HBASE_CLIENT_KEYVALUE_MAXSIZE)));
                        assertTrue(e.getMessage().contains(pk1Name));
                        assertTrue(e.getMessage().contains(pk2Name));
                        assertTrue(e.getMessage().contains(pk1Value));
                        assertTrue(e.getMessage().contains(pk2Value));
                        assertFalse(e.getMessage().contains(payload1Value));
                        assertFalse(e.getMessage().contains(payload3Value));
                    }
                }
            }
        }
    }
}
