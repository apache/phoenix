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

import static org.apache.phoenix.util.TestUtil.closeStmtAndConn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.phoenix.util.DateUtil;
import org.junit.Before;
import org.junit.Test;

public class CeilFunctionMillisEnd2EndIT extends BaseHBaseManagedTimeIT{
    
    private static long millisPart = 1000;
    private static int nanosPart = 500100;
    private static BigDecimal decimalUpserted = BigDecimal.valueOf(1.264);
    private static double doubleUpserted = 1.264d;
    private static double unsignedDoubleUpserted = 1.264d;
    private static float floatUpserted = 1.264f;
    private static float unsignedFloatUpserted = 1.264f;
    
    @Before
    public void initTable() throws Exception {
        String testString = "abc";
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            String ddl = "CREATE TABLE IF NOT EXISTS t1 (s VARCHAR NOT NULL PRIMARY KEY, dt DATE, t TIME, ts TIMESTAMP, dec DECIMAL, doub DOUBLE, undoub UNSIGNED_DOUBLE, fl FLOAT, unfl UNSIGNED_FLOAT)";
            conn.createStatement().execute(ddl);
            
            Date dateUpserted = DateUtil.parseDate("2012-01-01 14:25:28");
            dateUpserted = new Date(dateUpserted.getTime() + millisPart); // this makes the dateUpserted equivalent to 2012-01-01 14:25:28.660 
            long millis = dateUpserted.getTime();

            Time timeUpserted = new Time(millis);
            Timestamp tsUpserted = DateUtil.getTimestamp(millis, nanosPart);
            
            stmt =  conn.prepareStatement("UPSERT INTO t1 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
            stmt.setString(1, testString);
            stmt.setDate(2, dateUpserted);
            stmt.setTime(3, timeUpserted);
            stmt.setTimestamp(4, tsUpserted);
            stmt.setBigDecimal(5, decimalUpserted);
            stmt.setDouble(6, doubleUpserted);
            stmt.setDouble(7, unsignedDoubleUpserted);
            stmt.setFloat(8, floatUpserted);
            stmt.setFloat(9, unsignedFloatUpserted);
            stmt.executeUpdate();
            conn.commit();
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }

    @Test
    public void testCeilTimestamp() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT CEIL(ts, 'day'), CEIL(ts, 'hour'), CEIL(ts, 'minute'), CEIL(ts, 'second'), CEIL(ts, 'millisecond') FROM t1");
        assertTrue(rs.next());
        Timestamp expectedTimestamp;
        expectedTimestamp = new Timestamp(DateUtil.parseDate("2012-01-02 00:00:00").getTime());
        assertEquals(expectedTimestamp, rs.getTimestamp(1));
        expectedTimestamp = new Timestamp(DateUtil.parseDate("2012-01-01 15:00:00").getTime());
        assertEquals(expectedTimestamp, rs.getTimestamp(2));
        expectedTimestamp = new Timestamp(DateUtil.parseDate("2012-01-01 14:26:00").getTime());
        assertEquals(expectedTimestamp, rs.getTimestamp(3));
        expectedTimestamp = new Timestamp(DateUtil.parseDate("2012-01-01 14:25:29").getTime());
        assertEquals(expectedTimestamp, rs.getTimestamp(4));
        expectedTimestamp = new Timestamp(DateUtil.parseDate("2012-01-01 14:25:28").getTime() + millisPart + 1);
        assertEquals(expectedTimestamp, rs.getTimestamp(5));
    }
}
