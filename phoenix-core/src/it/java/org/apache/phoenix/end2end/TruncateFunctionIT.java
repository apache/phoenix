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

import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.ROW2;
import static org.apache.phoenix.util.TestUtil.ROW3;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.ParseException;

import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(ParallelStatsDisabledTest.class)
public class TruncateFunctionIT extends ParallelStatsDisabledIT {
    private static final String DS1 = "1970-01-10 00:58:01.587";
    private static final String DS2 = "1970-01-20 01:02:45.906";
    private static final String DS3 = "1970-01-30 01:30:24.353";
    
    private static Date toDate(String s) throws ParseException {
        return DateUtil.parseDate(s);
    }
    
    private static Timestamp toTimestamp(String s) throws ParseException {
        return DateUtil.parseTimestamp(s);
    }
    
    @Test
    public void testTruncate() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = generateUniqueName();
        ensureTableCreated(url, tableName, TestUtil.ATABLE_NAME, null, null, null);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " + tableName + 
                    "(" +
                    "    ORGANIZATION_ID, " +
                    "    ENTITY_ID, " +
                    "    A_DATE, " +
                    "    A_TIMESTAMP)" +
                    "VALUES (?, ?, ?, ?)");
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW1);
            stmt.setDate(3, toDate(DS1));
            stmt.setTimestamp(4, toTimestamp(DS1));
            stmt.execute();
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW2);
            stmt.setDate(3, toDate(DS2));
            stmt.setTimestamp(4, toTimestamp(DS2));
            stmt.execute();
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW3);
            stmt.setDate(3, toDate(DS3));
            stmt.setTimestamp(4, toTimestamp(DS3));
            stmt.execute();
            conn.commit();
            
            String query = "SELECT entity_id, trunc(a_date, 'day', 7), trunc(a_timestamp, 'second', 10) FROM " + tableName + " WHERE organization_id = ?";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            
            assertTrue (rs.next());
            assertEquals(ROW1, rs.getString(1));
            assertEquals(new Date((long) 7 * QueryConstants.MILLIS_IN_DAY), rs.getDate(2));
            assertEquals(toTimestamp("1970-01-10 00:58:00.000"), rs.getTimestamp(3));
            
            assertTrue (rs.next());
            assertEquals(ROW2, rs.getString(1));
            assertEquals(new Date((long) 14 * QueryConstants.MILLIS_IN_DAY), rs.getDate(2));
            assertEquals(toTimestamp("1970-01-20 01:02:40.000"), rs.getTimestamp(3));
            
            assertTrue (rs.next());
            assertEquals(ROW3, rs.getString(1));
            assertEquals(new Date((long) 28 * QueryConstants.MILLIS_IN_DAY), rs.getDate(2));
            assertEquals(toTimestamp("1970-01-30 01:30:20.000"), rs.getTimestamp(3));
            
            assertFalse(rs.next());
        }
    }

}

