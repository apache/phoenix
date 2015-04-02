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
import java.util.Properties;

import org.apache.phoenix.expression.function.CeilFunction;
import org.apache.phoenix.expression.function.FloorFunction;
import org.apache.phoenix.expression.function.RoundFunction;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.DateUtil;
import org.junit.Before;
import org.junit.Test;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;

/**
 * 
 * End to end tests for {@link RoundFunction}, {@link FloorFunction}, {@link CeilFunction} 
 *
 * 
 * @since 3.0.0
 */


public class RoundFloorCeilFunctionsEnd2EndIT extends BaseHBaseManagedTimeIT {
    
    private static long millisPart = 660;
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
    public void testRoundingUpDate() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT ROUND(dt, 'day'), ROUND(dt, 'hour', 1), ROUND(dt, 'minute', 1), ROUND(dt, 'second', 1), ROUND(dt, 'millisecond', 1) FROM t1");
        assertTrue(rs.next());
        Date expectedDate = DateUtil.parseDate("2012-01-02 00:00:00");
        assertEquals(expectedDate, rs.getDate(1));
        expectedDate = DateUtil.parseDate("2012-01-01 14:00:00");
        assertEquals(expectedDate, rs.getDate(2));
        expectedDate = DateUtil.parseDate("2012-01-01 14:25:00");
        assertEquals(expectedDate, rs.getDate(3));
        expectedDate = DateUtil.parseDate("2012-01-01 14:25:29");
        assertEquals(expectedDate, rs.getDate(4));
    }
    
    @Test
    public void testRoundingUpDateInWhere() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM t1 WHERE ROUND(dt, 'day') = to_date('2012-01-02 00:00:00')");
        assertTrue(rs.next());
    }
    
    @Test
    public void testFloorDate() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT FLOOR(dt, 'day', 1), FLOOR(dt, 'hour', 1), FLOOR(dt, 'minute', 1), FLOOR(dt, 'second', 1) FROM t1");
        assertTrue(rs.next());
        Date expectedDate = DateUtil.parseDate("2012-01-01 00:00:00");
        assertEquals(expectedDate, rs.getDate(1));
        expectedDate = DateUtil.parseDate("2012-01-01 14:00:00");
        assertEquals(expectedDate, rs.getDate(2));
        expectedDate = DateUtil.parseDate("2012-01-01 14:25:00");
        assertEquals(expectedDate, rs.getDate(3));
        expectedDate = DateUtil.parseDate("2012-01-01 14:25:28");
        assertEquals(expectedDate, rs.getDate(4));
    }
    
    @Test
    public void testFloorDateInWhere() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM t1 WHERE FLOOR(dt, 'hour') = to_date('2012-01-01 14:00:00')");
        assertTrue(rs.next());
    }
    
    @Test
    public void testCeilDate() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT CEIL(dt, 'day', 1), CEIL(dt, 'hour', 1), CEIL(dt, 'minute', 1), CEIL(dt, 'second', 1) FROM t1");
        assertTrue(rs.next());
        //Date upserted is 2012-01-01 14:25:28.660. So we will end up bumping up in every case.
        Date expectedDate = DateUtil.parseDate("2012-01-02 00:00:00");
        assertEquals(expectedDate, rs.getDate(1));
        expectedDate = DateUtil.parseDate("2012-01-01 15:00:00");
        assertEquals(expectedDate, rs.getDate(2));
        expectedDate = DateUtil.parseDate("2012-01-01 14:26:00");
        assertEquals(expectedDate, rs.getDate(3));
        expectedDate = DateUtil.parseDate("2012-01-01 14:25:29");
        assertEquals(expectedDate, rs.getDate(4));
    }
    
    @Test
    public void testCeilDateInWhere() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM t1 WHERE CEIL(dt, 'second') = to_date('2012-01-01 14:25:29')");
        assertTrue(rs.next());
    }
    
    @Test
    public void testRoundingUpTimestamp() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT ROUND(ts, 'day'), ROUND(ts, 'hour', 1), ROUND(ts, 'minute', 1), ROUND(ts, 'second', 1), ROUND(ts, 'millisecond', 1) FROM t1");
        assertTrue(rs.next());
        Timestamp expectedTimestamp;
        expectedTimestamp = new Timestamp(DateUtil.parseDate("2012-01-02 00:00:00").getTime());
        assertEquals(expectedTimestamp, rs.getTimestamp(1));
        expectedTimestamp = new Timestamp(DateUtil.parseDate("2012-01-01 14:00:00").getTime());
        assertEquals(expectedTimestamp, rs.getTimestamp(2));
        expectedTimestamp = new Timestamp(DateUtil.parseDate("2012-01-01 14:25:00").getTime());
        assertEquals(expectedTimestamp, rs.getTimestamp(3));
        expectedTimestamp = new Timestamp(DateUtil.parseDate("2012-01-01 14:25:29").getTime());
        assertEquals(expectedTimestamp, rs.getTimestamp(4));
        
        // Rounding of "2012-01-01 14:25:28.660" + nanosPart will end up bumping up the millisecond part of date. 
        // That is, it should be  evaluated as "2012-01-01 14:25:28.661". 
        expectedTimestamp = new Timestamp(DateUtil.parseDate("2012-01-01 14:25:28").getTime() + millisPart + 1);
        assertEquals(expectedTimestamp, rs.getTimestamp(5));
    }
    
    @Test
    public void testRoundingUpTimestampInWhere() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM t1 WHERE ROUND(ts, 'second') = to_date('2012-01-01 14:25:29')");
        assertTrue(rs.next());
    }
    
    @Test
    public void testFloorTimestamp() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT FLOOR(ts, 'day'), FLOOR(ts, 'hour', 1), FLOOR(ts, 'minute', 1), FLOOR(ts, 'second', 1), FLOOR(ts, 'millisecond', 1) FROM t1");
        assertTrue(rs.next());
        Timestamp expectedTimestamp;
        expectedTimestamp = new Timestamp(DateUtil.parseDate("2012-01-01 00:00:00").getTime());
        assertEquals(expectedTimestamp, rs.getTimestamp(1));
        expectedTimestamp = new Timestamp(DateUtil.parseDate("2012-01-01 14:00:00").getTime());
        assertEquals(expectedTimestamp, rs.getTimestamp(2));
        expectedTimestamp = new Timestamp(DateUtil.parseDate("2012-01-01 14:25:00").getTime());
        assertEquals(expectedTimestamp, rs.getTimestamp(3));
        expectedTimestamp = new Timestamp(DateUtil.parseDate("2012-01-01 14:25:28").getTime());
        assertEquals(expectedTimestamp, rs.getTimestamp(4));
        
        // FLOOR of "2012-01-01 14:25:28.660" + nanosPart will end up removing the nanos part. 
        expectedTimestamp = new Timestamp(DateUtil.parseDate("2012-01-01 14:25:28").getTime() + millisPart);
        assertEquals(expectedTimestamp, rs.getTimestamp(5));
    }
    
    @Test
    public void testFloorTimestampInWhere() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM t1 WHERE FLOOR(ts, 'second') = to_date('2012-01-01 14:25:28')");
        assertTrue(rs.next());
    }
    
    @Test
    public void testCeilTimestamp() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT CEIL(ts, 'day'), CEIL(ts, 'hour', 1), CEIL(ts, 'minute', 1), CEIL(ts, 'second', 1), CEIL(ts, 'millisecond', 1) FROM t1");
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
        
        // CEIL of "2012-01-01 14:25:28.660" + nanosPart will end up bumping up the millisecond part of date. 
        // That is, it should be  evaluated as "2012-01-01 14:25:28.661". 
        expectedTimestamp = new Timestamp(DateUtil.parseDate("2012-01-01 14:25:28").getTime() + millisPart + 1);
        assertEquals(expectedTimestamp, rs.getTimestamp(5));
    }
    
    @Test
    public void testCeilTimestampInWhere() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM t1 WHERE CEIL(ts, 'second') = to_date('2012-01-01 14:25:29')");
        assertTrue(rs.next());
    }
    
    @Test
    public void testRoundingUpTime() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT ROUND(t, 'day', 1), ROUND(t, 'hour', 1), ROUND(t, 'minute', 1), ROUND(t, 'second', 1) FROM t1");
        assertTrue(rs.next());
        Time expectedTime = new Time(DateUtil.parseDate("2012-01-02 00:00:00").getTime());
        assertEquals(expectedTime, rs.getTime(1));
        expectedTime = new Time(DateUtil.parseDate("2012-01-01 14:00:00").getTime());
        assertEquals(expectedTime, rs.getTime(2));
        expectedTime = new Time(DateUtil.parseDate("2012-01-01 14:25:00").getTime());
        assertEquals(expectedTime, rs.getTime(3));
        expectedTime = new Time(DateUtil.parseDate("2012-01-01 14:25:29").getTime());
        assertEquals(expectedTime, rs.getTime(4));
    }
    
    @Test
    public void testFloorTime() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT FLOOR(t, 'day', 1), FLOOR(t, 'hour', 1), FLOOR(t, 'minute', 1), FLOOR(t, 'second', 1) FROM t1");
        assertTrue(rs.next());
        Time expectedTime = new Time(DateUtil.parseDate("2012-01-01 00:00:00").getTime());
        assertEquals(expectedTime, rs.getTime(1));
        expectedTime = new Time(DateUtil.parseDate("2012-01-01 14:00:00").getTime());
        assertEquals(expectedTime, rs.getTime(2));
        expectedTime = new Time(DateUtil.parseDate("2012-01-01 14:25:00").getTime());
        assertEquals(expectedTime, rs.getTime(3));
        expectedTime = new Time(DateUtil.parseDate("2012-01-01 14:25:28").getTime());
        assertEquals(expectedTime, rs.getTime(4));
    }
    
    @Test
    public void testCeilTime() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT CEIL(t, 'day', 1), CEIL(t, 'hour', 1), CEIL(t, 'minute', 1), CEIL(t, 'second', 1) FROM t1");
        assertTrue(rs.next());
        Time expectedTime = new Time(DateUtil.parseDate("2012-01-02 00:00:00").getTime());
        assertEquals(expectedTime, rs.getTime(1));
        expectedTime = new Time(DateUtil.parseDate("2012-01-01 15:00:00").getTime());
        assertEquals(expectedTime, rs.getTime(2));
        expectedTime = new Time(DateUtil.parseDate("2012-01-01 14:26:00").getTime());
        assertEquals(expectedTime, rs.getTime(3));
        expectedTime = new Time(DateUtil.parseDate("2012-01-01 14:25:29").getTime());
        assertEquals(expectedTime, rs.getTime(4));
    }

    @Test
    public void testRoundingUpDecimal() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT ROUND(dec), ROUND(dec, 1), ROUND(dec, 2), ROUND(dec, 3) FROM t1");
        assertTrue(rs.next());
        BigDecimal expectedBd = BigDecimal.valueOf(1);
        assertEquals(expectedBd, rs.getBigDecimal(1));
        expectedBd = BigDecimal.valueOf(1.3);
        assertEquals(expectedBd, rs.getBigDecimal(2));
        expectedBd = BigDecimal.valueOf(1.26);
        assertEquals(expectedBd, rs.getBigDecimal(3));
        expectedBd = BigDecimal.valueOf(1.264);
        assertEquals(expectedBd, rs.getBigDecimal(4));
    }
    
    @Test
    public void testRoundingUpDecimalInWhere() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM t1 WHERE ROUND(dec, 2) = 1.26");
        assertTrue(rs.next());
    }
    
    @Test
    public void testFloorDecimal() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT FLOOR(dec), FLOOR(dec, 1), FLOOR(dec, 2), FLOOR(dec, 3) FROM t1");
        assertTrue(rs.next());
        BigDecimal expectedBd = BigDecimal.valueOf(1);
        assertEquals(expectedBd, rs.getBigDecimal(1));
        expectedBd = BigDecimal.valueOf(1.2);
        assertEquals(expectedBd, rs.getBigDecimal(2));
        expectedBd = BigDecimal.valueOf(1.26);
        assertEquals(expectedBd, rs.getBigDecimal(3));
        expectedBd = BigDecimal.valueOf(1.264);
        assertEquals(expectedBd, rs.getBigDecimal(4));
    }
    
    @Test
    public void testFloorDecimalInWhere() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM t1 WHERE FLOOR(dec, 2) = 1.26");
        assertTrue(rs.next());
    }
    
    @Test
    public void testCeilDecimal() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT CEIL(dec), CEIL(dec, 1), CEIL(dec, 2), CEIL(dec, 3) FROM t1");
        assertTrue(rs.next());
        BigDecimal expectedBd = BigDecimal.valueOf(2);
        assertEquals(expectedBd, rs.getBigDecimal(1));
        expectedBd = BigDecimal.valueOf(1.3);
        assertEquals(expectedBd, rs.getBigDecimal(2));
        expectedBd = BigDecimal.valueOf(1.27);
        assertEquals(expectedBd, rs.getBigDecimal(3));
        expectedBd = BigDecimal.valueOf(1.264);
        assertEquals(expectedBd, rs.getBigDecimal(4));
    }
    
    @Test
    public void testCeilDecimalInWhere() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM t1 WHERE CEIL(dec, 2) = 1.27");
        assertTrue(rs.next());
    }
    @Test
	public void testRoundingUpDouble() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		ResultSet rs = conn.createStatement().executeQuery("SELECT ROUND(doub), ROUND(doub, 1), ROUND(doub, 2), ROUND(doub, 3) FROM t1");
		assertTrue(rs.next());
		assertEquals(0, Doubles.compare(1, rs.getDouble(1)));
		assertEquals(0, Doubles.compare(1.3, rs.getDouble(2)));
		assertEquals(0, Doubles.compare(1.26, rs.getDouble(3)));
		assertEquals(0, Doubles.compare(1.264, rs.getDouble(4)));
	}

	@Test
	public void testRoundingUpDoubleInWhere() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM t1 WHERE ROUND(dec, 2) = 1.26");
		assertTrue(rs.next());
	}

	@Test
	public void testCeilDouble() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		ResultSet rs = conn.createStatement().executeQuery("SELECT CEIL(doub), CEIL(doub, 1), CEIL(doub, 2), CEIL(doub, 3) FROM t1");
		assertTrue(rs.next());
		assertEquals(0, Doubles.compare(2, rs.getDouble(1)));
		assertEquals(0, Doubles.compare(1.3, rs.getDouble(2)));
		assertEquals(0, Doubles.compare(1.27, rs.getDouble(3)));
		assertEquals(0, Doubles.compare(1.264, rs.getDouble(4)));
	}

	@Test
	public void testCeilDoubleInWhere() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM t1 WHERE CEIL(doub, 2) = 1.27");
		assertTrue(rs.next());
	}

	@Test
	public void testFloorDouble() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		ResultSet rs = conn.createStatement().executeQuery("SELECT FLOOR(doub), FLOOR(doub, 1), FLOOR(doub, 2), FLOOR(doub, 3) FROM t1");
		assertTrue(rs.next());
		assertEquals(0, Doubles.compare(1, rs.getDouble(1)));
		assertEquals(0, Doubles.compare(1.2, rs.getDouble(2)));
		assertEquals(0, Doubles.compare(1.26, rs.getDouble(3)));
		assertEquals(0, Doubles.compare(1.264, rs.getDouble(4)));
	}

	@Test
	public void testFloorDoubleInWhere() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM t1 WHERE FLOOR(doub, 2) = 1.26");
		assertTrue(rs.next());
	}
	
	@Test
	public void testRoundFloat() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		ResultSet rs = conn.createStatement().executeQuery("SELECT ROUND(fl), ROUND(fl, 1), ROUND(fl, 2), ROUND(fl, 3) FROM t1");
		assertTrue(rs.next());
		assertEquals(0, Floats.compare(1, rs.getFloat(1)));
		assertEquals(0, Floats.compare(1.3f, rs.getFloat(2)));
		assertEquals(0, Floats.compare(1.26f, rs.getFloat(3)));
		assertEquals(0, Floats.compare(1.264f, rs.getFloat(4)));
	}
	
	@Test
	public void testRoundUnsignedFloat() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		ResultSet rs = conn.createStatement().executeQuery("SELECT ROUND(unfl), ROUND(unfl, 1), ROUND(unfl, 2), ROUND(unfl, 3) FROM t1");
		assertTrue(rs.next());
		assertEquals(0, Floats.compare(1, rs.getFloat(1)));
		assertEquals(0, Floats.compare(1.3f, rs.getFloat(2)));
		assertEquals(0, Floats.compare(1.26f, rs.getFloat(3)));
		assertEquals(0, Floats.compare(1.264f, rs.getFloat(4)));
	}
	
	@Test
	public void testRoundUnsignedDouble() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		ResultSet rs = conn.createStatement().executeQuery("SELECT ROUND(undoub), ROUND(undoub, 1), ROUND(undoub, 2), ROUND(undoub, 3) FROM t1");
		assertTrue(rs.next());
		assertEquals(0, Floats.compare(1, rs.getFloat(1)));
		assertEquals(0, Floats.compare(1.3f, rs.getFloat(2)));
		assertEquals(0, Floats.compare(1.26f, rs.getFloat(3)));
		assertEquals(0, Floats.compare(1.264f, rs.getFloat(4)));
	}	
	
	@Test
	public void testTimestampAggregateFunctions() throws Exception {
		String dateString = "2015-03-08 09:09:11.665";
		Properties props = new Properties();
		props.setProperty(QueryServices.DATE_FORMAT_TIMEZONE_ATTRIB, "GMT+1");
		Connection conn = DriverManager.getConnection(getUrl(), props);
		try {
			conn.prepareStatement(
					"create table TIME_AGG_TABLE("
							+ "ID unsigned_int NOT NULL, "
							+ "THE_DATE TIMESTAMP, "
							+ "constraint PK primary key (ID))").execute();
			PreparedStatement stmt = conn.prepareStatement("upsert into "
					+ "TIME_AGG_TABLE(" + "    ID, " + "    THE_DATE)"
					+ "VALUES (?, ?)");
			stmt.setInt(1, 1);
			stmt.setTimestamp(2, DateUtil.parseTimestamp(dateString));
			stmt.execute();
			conn.commit();

			ResultSet rs = conn.prepareStatement(
					"SELECT THE_DATE ,TRUNC(THE_DATE,'DAY') AS day_from_dt "
							+ ",TRUNC(THE_DATE,'HOUR') AS hour_from_dt "
							+ ",TRUNC(THE_DATE,'MINUTE') AS min_from_dt "
							+ ",TRUNC(THE_DATE,'SECOND') AS sec_from_dt "
							+ ",TRUNC(THE_DATE,'MILLISECOND') AS mil_from_dt "
							+ "FROM TIME_AGG_TABLE").executeQuery();
			assertTrue(rs.next());
			assertEquals(DateUtil.parseTimestamp("2015-03-08 09:09:11.665"),
					rs.getTimestamp("THE_DATE"));
			assertEquals(DateUtil.parseTimestamp("2015-03-08 00:00:00.0"),
					rs.getTimestamp("day_from_dt"));
			assertEquals(DateUtil.parseTimestamp("2015-03-08 09:00:00.0"),
					rs.getTimestamp("hour_from_dt"));
			assertEquals(DateUtil.parseTimestamp("2015-03-08 09:09:00.0"),
					rs.getTimestamp("min_from_dt"));
			assertEquals(DateUtil.parseTimestamp("2015-03-08 09:09:11.0"),
					rs.getTimestamp("sec_from_dt"));
			assertEquals(DateUtil.parseTimestamp("2015-03-08 09:09:11.665"),
					rs.getTimestamp("mil_from_dt"));
			rs.close();

			rs = conn.prepareStatement(
					"SELECT THE_DATE ,ROUND(THE_DATE,'DAY') AS day_from_dt "
							+ ",ROUND(THE_DATE,'HOUR') AS hour_from_dt "
							+ ",ROUND(THE_DATE,'MINUTE') AS min_from_dt "
							+ ",ROUND(THE_DATE,'SECOND') AS sec_from_dt "
							+ ",ROUND(THE_DATE,'MILLISECOND') AS mil_from_dt "
							+ "FROM TIME_AGG_TABLE").executeQuery();
			assertTrue(rs.next());
			assertEquals(DateUtil.parseTimestamp("2015-03-08 09:09:11.665"),
					rs.getTimestamp("THE_DATE"));
			assertEquals(DateUtil.parseTimestamp("2015-03-08 00:00:00.0"),
					rs.getTimestamp("day_from_dt"));
			assertEquals(DateUtil.parseTimestamp("2015-03-08 09:00:00.0"),
					rs.getTimestamp("hour_from_dt"));
			assertEquals(DateUtil.parseTimestamp("2015-03-08 09:09:00.0"),
					rs.getTimestamp("min_from_dt"));
			assertEquals(DateUtil.parseTimestamp("2015-03-08 09:09:12.0"),
					rs.getTimestamp("sec_from_dt"));
			assertEquals(DateUtil.parseTimestamp("2015-03-08 09:09:11.665"),
					rs.getTimestamp("mil_from_dt"));
			rs.close();

			rs = conn.prepareStatement(
					"SELECT THE_DATE ,FLOOR(THE_DATE,'DAY') AS day_from_dt "
							+ ",FLOOR(THE_DATE,'HOUR') AS hour_from_dt "
							+ ",FLOOR(THE_DATE,'MINUTE') AS min_from_dt "
							+ ",FLOOR(THE_DATE,'SECOND') AS sec_from_dt "
							+ ",FLOOR(THE_DATE,'MILLISECOND') AS mil_from_dt "
							+ "FROM TIME_AGG_TABLE").executeQuery();
			assertTrue(rs.next());
			assertEquals(DateUtil.parseTimestamp("2015-03-08 09:09:11.665"),
					rs.getTimestamp("THE_DATE"));
			assertEquals(DateUtil.parseTimestamp("2015-03-08 00:00:00.0"),
					rs.getTimestamp("day_from_dt"));
			assertEquals(DateUtil.parseTimestamp("2015-03-08 09:00:00.0"),
					rs.getTimestamp("hour_from_dt"));
			assertEquals(DateUtil.parseTimestamp("2015-03-08 09:09:00.0"),
					rs.getTimestamp("min_from_dt"));
			assertEquals(DateUtil.parseTimestamp("2015-03-08 09:09:11.0"),
					rs.getTimestamp("sec_from_dt"));
			assertEquals(DateUtil.parseTimestamp("2015-03-08 09:09:11.665"),
					rs.getTimestamp("mil_from_dt"));
			rs.close();

			rs = conn.prepareStatement(
					"SELECT THE_DATE ,CEIL(THE_DATE,'DAY') AS day_from_dt "
							+ ",CEIL(THE_DATE,'HOUR') AS hour_from_dt "
							+ ",CEIL(THE_DATE,'MINUTE') AS min_from_dt "
							+ ",CEIL(THE_DATE,'SECOND') AS sec_from_dt "
							+ ",CEIL(THE_DATE,'MILLISECOND') AS mil_from_dt "
							+ "FROM TIME_AGG_TABLE").executeQuery();
			assertTrue(rs.next());
			assertEquals(DateUtil.parseTimestamp("2015-03-08 09:09:11.665"),
					rs.getTimestamp("THE_DATE"));
			assertEquals(DateUtil.parseTimestamp("2015-03-09 00:00:00.0"),
					rs.getTimestamp("day_from_dt"));
			assertEquals(DateUtil.parseTimestamp("2015-03-08 10:00:00.0"),
					rs.getTimestamp("hour_from_dt"));
			assertEquals(DateUtil.parseTimestamp("2015-03-08 09:10:00.0"),
					rs.getTimestamp("min_from_dt"));
			assertEquals(DateUtil.parseTimestamp("2015-03-08 09:09:12.0"),
					rs.getTimestamp("sec_from_dt"));
			assertEquals(DateUtil.parseTimestamp("2015-03-08 09:09:11.665"),
					rs.getTimestamp("mil_from_dt"));
			rs.close();
		} finally {
			conn.close();
		}
	}

}
