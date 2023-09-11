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

import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.phoenix.exception.SQLExceptionCode.OPERATION_TIMED_OUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(ParallelStatsDisabledTest.class)
public class OperationTimeoutWithReasonIT extends ParallelStatsDisabledIT {

    private static final class MyClock extends EnvironmentEdge {
        private long time;
        private final long delay;

        public MyClock (long time, long delay) {
            this.time = time;
            this.delay = delay;
        }

        @Override
        public long currentTime() {
            long currentTime = this.time;
            this.time += this.delay;
            return currentTime;
        }
    }

    @Test
    public void testOperationTimeout() throws SQLException {
        final String tableName = generateUniqueName();
        final String ddl = "CREATE TABLE " + tableName
            + " (COL1 VARCHAR NOT NULL PRIMARY KEY, COL2 VARCHAR)";
        try (Connection conn = DriverManager.getConnection(getUrl());
                 Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);
            final String dml = String.format("UPSERT INTO %s VALUES (?, ?)",
                tableName);
            try(PreparedStatement prepStmt = conn.prepareStatement(dml)) {
                for (int i = 1; i <= 100; i++) {
                    prepStmt.setString(1, "key" + i);
                    prepStmt.setString(2, "value" + i);
                    prepStmt.executeUpdate();
                }
            }
            conn.commit();
        }

        try (Connection conn = DriverManager.getConnection(getUrl());
             Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(5); // 5 sec
            ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s",
                tableName));
            // Use custom EnvironmentEdge to timeout query with a longer delay in ms
            MyClock clock = new MyClock(10, 10000);
            EnvironmentEdgeManager.injectEdge(clock);
            try {
                rs.next();
                fail();
            } catch (SQLException e) {
                assertEquals(OPERATION_TIMED_OUT.getErrorCode(),
                    e.getErrorCode());
                assertTrue(e.getMessage().contains("Query couldn't be " +
                    "completed in the allotted time: 5000 ms"));
            }
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

}