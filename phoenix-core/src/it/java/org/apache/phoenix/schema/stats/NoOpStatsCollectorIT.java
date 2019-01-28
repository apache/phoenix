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
package org.apache.phoenix.schema.stats;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

/**
 * Tests the behavior of stats collection code when stats are disabled on server side
 */
public class NoOpStatsCollectorIT extends ParallelStatsDisabledIT {

    private static final Log LOG = LogFactory.getLog(NoOpStatsCollectorIT.class);

    private String fullTableName;
    private String physicalTableName;
    private Connection conn;

    @Before
    public void beforeTest() throws SQLException {
        String schemaName = generateUniqueName();
        String tableName = "T_" + generateUniqueName();
        fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        physicalTableName = SchemaUtil.getPhysicalHBaseTableName(schemaName,
                tableName, false).getString();
        conn = getConnection();
        conn.createStatement().execute(
                "CREATE TABLE " + fullTableName + " ( k VARCHAR, a_string_array VARCHAR(100) ARRAY[4],"
                        + " b_string_array VARCHAR(100) ARRAY[4] \n"
                        + " CONSTRAINT pk PRIMARY KEY (k, b_string_array DESC)) GUIDE_POSTS_WIDTH = 10");
        upsertValues(conn, fullTableName);
    }

    /**
     * Update Statistics SQL statement should be disallowed
     * when QueryServices#STATS_COLLECTION_ENABLED is disabled on server
     */
    @Test
    public void testStatsCollectionViaSql() throws SQLException {
        String updateStatisticsSql = "UPDATE STATISTICS " + fullTableName;
        LOG.info("Running SQL to collect stats: " + updateStatisticsSql);
        Statement stmt = conn.createStatement();
        try {
            stmt.execute(updateStatisticsSql);
        } catch (SQLException e) {
            Assert.assertEquals("StatsCollectionDisabledOnServerException expected",
                    1401, e.getErrorCode());
            Assert.assertEquals("StatsCollectionDisabledOnServerException expected",
                    "STS01", e.getSQLState());
        }
    }

    /**
     * Major compaction should not compute / persist statistics
     * when QueryServices#STATS_COLLECTION_ENABLED is disabled on server
     */
    @Test
    public void testStatsCollectionDuringMajorCompaction() throws Exception {
        LOG.info("Running major compaction on table: " + physicalTableName);
        TestUtil.doMajorCompaction(conn, physicalTableName);

        String q1 = "SELECT SUM(GUIDE_POSTS_ROW_COUNT) FROM SYSTEM.STATS WHERE PHYSICAL_NAME = '" + physicalTableName + "'";
        ResultSet rs1 = conn.createStatement().executeQuery(q1);
        rs1.next();
        int rowCountFromStats = rs1.getInt(1);

        String q2 = "SELECT COUNT(*) FROM " + fullTableName;
        ResultSet rs2 = conn.createStatement().executeQuery(q2);
        rs2.next();
        int rowCountFromTable = rs2.getInt(1);

        Assert.assertTrue("Stats collection is disabled, hence row counts should not match",
                rowCountFromStats != rowCountFromTable);
        Assert.assertEquals("Stats collection is disabled, hence row counts from stats should be 0",
                0, rowCountFromStats);
    }

    private Connection getConnection() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        return DriverManager.getConnection(getUrl(), props);
    }

    private void upsertValues(Connection conn, String tableName) throws SQLException {
        PreparedStatement stmt = upsertStmt(conn, tableName);
        stmt.setString(1, "a");
        String[] s = new String[] { "abc", "def", "ghi", "jkll", null, null, "xxx" };
        Array array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "abc", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
    }

    private PreparedStatement upsertStmt(Connection conn, String tableName) throws SQLException {
        PreparedStatement stmt;
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?,?)");
        return stmt;
    }

}
