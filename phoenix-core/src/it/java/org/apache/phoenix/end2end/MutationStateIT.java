package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

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
    public void testMaxMutationSize() throws Exception {
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
    
}
