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

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Properties;

import static org.apache.phoenix.query.QueryConstants.MILLIS_IN_DAY;
import static org.apache.phoenix.util.TestUtil.ATABLE_NAME;
import static org.apache.phoenix.util.TestUtil.A_VALUE;
import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.C_VALUE;
import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.ROW2;
import static org.apache.phoenix.util.TestUtil.ROW3;
import static org.apache.phoenix.util.TestUtil.ROW4;
import static org.apache.phoenix.util.TestUtil.ROW5;
import static org.apache.phoenix.util.TestUtil.ROW6;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.ROW8;
import static org.apache.phoenix.util.TestUtil.ROW9;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DistinctSumIT extends BaseClientManagedTimeIT {

    @Test public void testDistinctSumOnColumn() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);

        String query = "SELECT sum(DISTINCT A_INTEGER) FROM aTable";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    protected static void initATableValues(String tenantId, byte[][] splits, Date date, Long ts)
            throws Exception {
        if (ts == null) {
            ensureTableCreated(getUrl(), ATABLE_NAME, splits);
        } else {
            ensureTableCreated(getUrl(), ATABLE_NAME, splits, ts - 2);
        }

        Properties props = new Properties();
        if (ts != null) {
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, ts.toString());
        }
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            // Insert all rows at ts
            PreparedStatement stmt = conn.prepareStatement("upsert into " +
                    "ATABLE(" +
                    "    ORGANIZATION_ID, " +
                    "    ENTITY_ID, " +
                    "    A_STRING, " +
                    "    B_STRING, " +
                    "    A_INTEGER, " +
                    "    A_DATE, " +
                    "    X_DECIMAL, " +
                    "    X_LONG, " +
                    "    X_INTEGER," +
                    "    Y_INTEGER)" +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW1);
            stmt.setString(3, A_VALUE);
            stmt.setString(4, B_VALUE);
            stmt.setInt(5, 1);
            stmt.setDate(6, date);
            stmt.setBigDecimal(7, null);
            stmt.setNull(8, Types.BIGINT);
            stmt.setNull(9, Types.INTEGER);
            stmt.setNull(10, Types.INTEGER);
            stmt.execute();

            stmt.setString(1, tenantId);
            stmt.setString(2, ROW2);
            stmt.setString(3, A_VALUE);
            stmt.setString(4, C_VALUE);
            stmt.setInt(5, 2);
            stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 1));
            stmt.setBigDecimal(7, null);
            stmt.setNull(8, Types.BIGINT);
            stmt.setNull(9, Types.INTEGER);
            stmt.setNull(10, Types.INTEGER);
            stmt.execute();

            stmt.setString(1, tenantId);
            stmt.setString(2, ROW3);
            stmt.setString(3, A_VALUE);
            stmt.setString(4, C_VALUE);
            stmt.setInt(5, 1);
            stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 2));
            stmt.setBigDecimal(7, null);
            stmt.setNull(8, Types.BIGINT);
            stmt.setNull(9, Types.INTEGER);
            stmt.setNull(10, Types.INTEGER);
            stmt.execute();

            conn.commit();
        } finally {
            conn.close();
        }
    }
}
