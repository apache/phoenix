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

import org.apache.phoenix.util.MergeViewIndexIdSequencesTool;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

@Category(NeedsOwnMiniClusterTest.class)
public class MergeViewIndexIdSequencesToolIT extends ParallelStatsDisabledIT {
    private final String CLEAN_QUERY = "DELETE FROM SYSTEM.\"SEQUENCE\"";
    private final String COUNT_QUERY = "SELECT COUNT(*) FROM SYSTEM.\"SEQUENCE\"";
    private final String UPSERT_QUERY = "UPSERT INTO SYSTEM.\"SEQUENCE\" " +
            "(TENANT_ID, SEQUENCE_SCHEMA, SEQUENCE_NAME, START_WITH, CURRENT_VALUE, INCREMENT_BY," +
            "CACHE_SIZE,MIN_VALUE,MAX_VALUE,CYCLE_FLAG,LIMIT_REACHED_FLAG) VALUES " +
            "(?,?,?,?,?,?,?,?,?,?,?)";
    private final String OLD_SEQUENCE_SCHEMA = "_SEQ_TEST.B";
    private final String OLD_SEQUENCE_NAME = "_ID_";
    private final String NEW_SEQUENCE_SCHEMA = "TEST";
    private final String NEW_SEQUENCE_NAME = "B_ID_";

    @Test
    public void testOldSequenceFormat() throws Exception {
        testSequenceRowCount(true);
    }

    @Test
    public void testNewSequenceFormat() throws Exception {
        testSequenceRowCount(false);
    }

    private void testSequenceRowCount(boolean isTestingOldFormat) throws Exception {
        int expectedRowCount = isTestingOldFormat ? 2 : 1;
        MergeViewIndexIdSequencesTool tool = new MergeViewIndexIdSequencesTool();
        tool.setConf(config);
        try(Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(CLEAN_QUERY);
            try (PreparedStatement preparedStatement = conn.prepareStatement(UPSERT_QUERY)) {
                preparedStatement.setString(1, null);
                if (isTestingOldFormat) {
                    preparedStatement.setString(2, OLD_SEQUENCE_SCHEMA);
                    preparedStatement.setString(3, OLD_SEQUENCE_NAME);
                } else {
                    preparedStatement.setString(2, NEW_SEQUENCE_SCHEMA);
                    preparedStatement.setString(3, NEW_SEQUENCE_NAME);
                }
                preparedStatement.setLong(4, Short.MIN_VALUE);
                preparedStatement.setLong(5, Short.MIN_VALUE + 1);
                preparedStatement.setInt(6, 1);
                preparedStatement.setInt(7, 1);
                preparedStatement.setLong(8, Long.MIN_VALUE);
                preparedStatement.setLong(9, Long.MAX_VALUE);
                preparedStatement.setBoolean(10, false);
                preparedStatement.setBoolean(11, false);
                preparedStatement.execute();
                conn.commit();
            }
            ResultSet rs = conn.createStatement().executeQuery(COUNT_QUERY);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));

            int status = tool.run(new String[]{"-r"});
            assertEquals(0, status);

            rs = conn.createStatement().executeQuery(COUNT_QUERY);
            assertTrue(rs.next());
            assertEquals(expectedRowCount, rs.getInt(1));
        }
    }
}