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

import static org.apache.phoenix.util.TestUtil.ATABLE_NAME;
import static org.apache.phoenix.util.TestUtil.A_VALUE;
import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.C_VALUE;
import static org.apache.phoenix.util.TestUtil.INDEX_DATA_SCHEMA;
import static org.apache.phoenix.util.TestUtil.INDEX_DATA_TABLE;
import static org.apache.phoenix.util.TestUtil.MILLIS_IN_DAY;
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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;

import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;


public class PercentileIT extends BaseClientManagedTimeIT {

    @Test
    public void testPercentile() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, null, getDefaultSplits(tenantId), null, ts);

        String query = "SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY A_INTEGER ASC) FROM aTable";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at
                                                                                     // timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            BigDecimal percentile = rs.getBigDecimal(1);
            percentile = percentile.setScale(1, RoundingMode.HALF_UP);
            assertEquals(8.6, percentile.doubleValue(),0.0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testPercentileDesc() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, null, getDefaultSplits(tenantId), null, ts);

        String query = "SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY A_INTEGER DESC) FROM aTable";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at
                                                                                     // timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            BigDecimal percentile = rs.getBigDecimal(1);
            percentile = percentile.setScale(1, RoundingMode.HALF_UP);
            assertEquals(1.4, percentile.doubleValue(),0.0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testPercentileWithGroupby() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, null, getDefaultSplits(tenantId), null, ts);

        String query = "SELECT A_STRING, PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY A_INTEGER ASC) FROM aTable GROUP BY A_STRING";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at
                                                                                     // timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            BigDecimal percentile = rs.getBigDecimal(2);
            percentile = percentile.setScale(1, RoundingMode.HALF_UP);
            assertEquals(7.0, percentile.doubleValue(),0.0);
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            percentile = rs.getBigDecimal(2);
            percentile = percentile.setScale(1, RoundingMode.HALF_UP);
            assertEquals(9.0, percentile.doubleValue(),0.0);
            assertTrue(rs.next());
            assertEquals("c",rs.getString(1));
            percentile = rs.getBigDecimal(2);
            percentile = percentile.setScale(1, RoundingMode.HALF_UP);
            assertEquals(8.0, percentile.doubleValue(),0.0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testPercentileWithGroupbyAndOrderBy() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, null, getDefaultSplits(tenantId), null, ts);

        String query = "SELECT A_STRING, PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY A_INTEGER ASC) AS PC FROM aTable GROUP BY A_STRING ORDER BY PC";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at
                                                                                     // timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            BigDecimal percentile = rs.getBigDecimal(2);
            percentile = percentile.setScale(1, RoundingMode.HALF_UP);
            assertEquals(7.0, percentile.doubleValue(),0.0);
            assertTrue(rs.next());
            assertEquals("c",rs.getString(1));
            percentile = rs.getBigDecimal(2);
            percentile = percentile.setScale(1, RoundingMode.HALF_UP);
            assertEquals(8.0, percentile.doubleValue(),0.0);
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            percentile = rs.getBigDecimal(2);
            percentile = percentile.setScale(1, RoundingMode.HALF_UP);
            assertEquals(9.0, percentile.doubleValue(),0.0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
	public void testPercentileDiscAsc() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		initATableValues(tenantId, null, getDefaultSplits(tenantId), null, ts);

		String query = "SELECT PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY A_INTEGER ASC) FROM aTable";

		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at
										// timestamp 2
		Connection conn = DriverManager.getConnection(getUrl(), props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			int percentile_disc = rs.getInt(1);
			assertEquals(9, percentile_disc);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}
	
	@Test
	public void testPercentileDiscDesc() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		initATableValues(tenantId, null, getDefaultSplits(tenantId), null, ts);

		String query = "SELECT PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY A_INTEGER DESC) FROM aTable";

		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at
										// timestamp 2
		Connection conn = DriverManager.getConnection(getUrl(), props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			int percentile_disc = rs.getInt(1);
			assertEquals(1, percentile_disc);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}
    
    @Test
    public void testPercentileDiscWithGroupby() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, null, getDefaultSplits(tenantId), null, ts);

        String query = "SELECT A_STRING, PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY A_INTEGER ASC) FROM aTable GROUP BY A_STRING";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at
                                                                                     // timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            int percentile_disc = rs.getInt(2);
            assertEquals(2, percentile_disc);
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            percentile_disc = rs.getInt(2);
            assertEquals(5, percentile_disc);
            assertTrue(rs.next());
            assertEquals("c",rs.getString(1));
            percentile_disc = rs.getInt(2);
            assertEquals(8, percentile_disc);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testPercentileDiscWithGroupbyAndOrderBy() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, null, getDefaultSplits(tenantId), null, ts);

        String query = "SELECT A_STRING, PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY A_INTEGER ASC) FROM aTable GROUP BY A_STRING ORDER BY A_STRING DESC";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at
                                                                                     // timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("c",rs.getString(1));
            int percentile_disc = rs.getInt(2);
            assertEquals(8, percentile_disc);
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            percentile_disc = rs.getInt(2);
            assertEquals(5, percentile_disc);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            percentile_disc = rs.getInt(2);
            assertEquals(2, percentile_disc);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testPercentRank() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, null, getDefaultSplits(tenantId), null, ts);

        String query = "SELECT PERCENT_RANK(5) WITHIN GROUP (ORDER BY A_INTEGER ASC) FROM aTable";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at
                                                                                     // timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            BigDecimal rank = rs.getBigDecimal(1);
            rank = rank.setScale(2, RoundingMode.HALF_UP);
            assertEquals(0.56, rank.doubleValue(), 0.0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testPercentRankWithNegativeNumeric() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, null, getDefaultSplits(tenantId), null, ts);

        String query = "SELECT PERCENT_RANK(-2) WITHIN GROUP (ORDER BY A_INTEGER ASC) FROM aTable";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at
                                                                                     // timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            BigDecimal rank = rs.getBigDecimal(1);
            rank = rank.setScale(2, RoundingMode.HALF_UP);
            assertEquals(0.0, rank.doubleValue(), 0.0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testPercentRankDesc() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, null, getDefaultSplits(tenantId), null, ts);

        String query = "SELECT PERCENT_RANK(8.9) WITHIN GROUP (ORDER BY A_INTEGER DESC) FROM aTable";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at
                                                                                     // timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            BigDecimal rank = rs.getBigDecimal(1);
            rank = rank.setScale(2, RoundingMode.HALF_UP);
            assertEquals(0.11, rank.doubleValue(), 0.0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testPercentRankDescOnVARCHARColumn() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, null, getDefaultSplits(tenantId), null, ts);

        String query = "SELECT PERCENT_RANK('ba') WITHIN GROUP (ORDER BY A_STRING DESC) FROM aTable";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at
                                                                                     // timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            BigDecimal rank = rs.getBigDecimal(1);
            rank = rank.setScale(2, RoundingMode.HALF_UP);
            assertEquals(0.11, rank.doubleValue(), 0.0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testPercentRankDescOnDECIMALColumn() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, null, getDefaultSplits(tenantId), null, ts);

        String query = "SELECT PERCENT_RANK(2) WITHIN GROUP (ORDER BY x_decimal ASC) FROM aTable";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at
                                                                                     // timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            BigDecimal rank = rs.getBigDecimal(1);
            rank = rank.setScale(2, RoundingMode.HALF_UP);
            assertEquals(0.33, rank.doubleValue(), 0.0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testMultiplePercentRanksOnSelect() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, null, getDefaultSplits(tenantId), null, ts);

        String query = "SELECT PERCENT_RANK(2) WITHIN GROUP (ORDER BY x_decimal ASC), PERCENT_RANK(8.9) WITHIN GROUP (ORDER BY A_INTEGER DESC) FROM aTable";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at
                                                                                     // timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            BigDecimal rank = rs.getBigDecimal(1);
            rank = rank.setScale(2, RoundingMode.HALF_UP);
            assertEquals(0.33, rank.doubleValue(), 0.0);
            rank = rs.getBigDecimal(2);
            rank = rank.setScale(2, RoundingMode.HALF_UP);
            assertEquals(0.11, rank.doubleValue(), 0.0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testPercentileContOnDescPKColumn() throws Exception {
        ensureTableCreated(getUrl(), INDEX_DATA_TABLE);
        populateINDEX_DATA_TABLETable();

        String query = "SELECT PERCENTILE_CONT(1) WITHIN GROUP (ORDER BY long_pk ASC) FROM " + INDEX_DATA_SCHEMA
                + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE;

        Connection conn = DriverManager.getConnection(getUrl());
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            BigDecimal percentile = rs.getBigDecimal(1);
            percentile = percentile.setScale(1, RoundingMode.HALF_UP);
            assertEquals(3.0, percentile.doubleValue(),0.0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testPercentRankOnDescPKColumn() throws Exception {
        ensureTableCreated(getUrl(), INDEX_DATA_TABLE);
        populateINDEX_DATA_TABLETable();

        String query = "SELECT PERCENT_RANK(2) WITHIN GROUP (ORDER BY long_pk ASC) FROM " + INDEX_DATA_SCHEMA
                + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE;

        Connection conn = DriverManager.getConnection(getUrl());
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            BigDecimal rank = rs.getBigDecimal(1);
            rank = rank.setScale(2, RoundingMode.HALF_UP);
            assertEquals(0.67, rank.doubleValue(), 0.0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testPercentileDiscOnDescPKColumn() throws Exception {
        ensureTableCreated(getUrl(), INDEX_DATA_TABLE);
        populateINDEX_DATA_TABLETable();

        String query = "SELECT PERCENTILE_DISC(0.4) WITHIN GROUP (ORDER BY long_pk DESC) FROM " + INDEX_DATA_SCHEMA
                + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE;

        Connection conn = DriverManager.getConnection(getUrl());
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            long percentile_disc = rs.getLong(1);
            assertEquals(2, percentile_disc);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    private static void populateINDEX_DATA_TABLETable() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        Date date = DateUtil.parseDate("2015-01-01 00:00:00");
        try {
            String upsert = "UPSERT INTO " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE
                    + " VALUES(?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(upsert);
            stmt.setString(1, "varchar1");
            stmt.setString(2, "char1");
            stmt.setInt(3, 1);
            stmt.setLong(4, 1L);
            stmt.setBigDecimal(5, new BigDecimal(1.0));
            stmt.setDate(6, date);
            stmt.executeUpdate();
            
            stmt.setString(1, "varchar2");
            stmt.setString(2, "char2");
            stmt.setInt(3, 2);
            stmt.setLong(4, 2L);
            stmt.setBigDecimal(5, new BigDecimal(2.0));
            stmt.setDate(6, date);
            stmt.executeUpdate();
            
            stmt.setString(1, "varchar3");
            stmt.setString(2, "char3");
            stmt.setInt(3, 3);
            stmt.setLong(4, 3L);
            stmt.setBigDecimal(5, new BigDecimal(3.0));
            stmt.setDate(6, date);
            stmt.executeUpdate();
            
            conn.commit();
        } finally {
            conn.close();
        }
    }

    protected static void initATableValues(String tenantId1, String tenantId2, byte[][] splits,
            Date date, Long ts) throws Exception {
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
            PreparedStatement stmt = conn.prepareStatement("upsert into " + "ATABLE("
                    + "    ORGANIZATION_ID, " + "    ENTITY_ID, " + "    A_STRING, "
                    + "    B_STRING, " + "    A_INTEGER, " + "    A_DATE, " + "    X_DECIMAL, "
                    + "    X_LONG, " + "    X_INTEGER," + "    Y_INTEGER)"
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            if (tenantId1 != null) {
                stmt.setString(1, tenantId1);
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

                stmt.setString(1, tenantId1);
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

                stmt.setString(1, tenantId1);
                stmt.setString(2, ROW3);
                stmt.setString(3, A_VALUE);
                stmt.setString(4, C_VALUE);
                stmt.setInt(5, 3);
                stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 2));
                stmt.setBigDecimal(7, null);
                stmt.setNull(8, Types.BIGINT);
                stmt.setNull(9, Types.INTEGER);
                stmt.setNull(10, Types.INTEGER);
                stmt.execute();

                stmt.setString(1, tenantId1);
                stmt.setString(2, ROW4);
                stmt.setString(3, A_VALUE);
                stmt.setString(4, B_VALUE);
                stmt.setInt(5, 7);
                stmt.setDate(6, date == null ? null : date);
                stmt.setBigDecimal(7, null);
                stmt.setNull(8, Types.BIGINT);
                stmt.setNull(9, Types.INTEGER);
                stmt.setNull(10, Types.INTEGER);
                stmt.execute();

                stmt.setString(1, tenantId1);
                stmt.setString(2, ROW5);
                stmt.setString(3, B_VALUE);
                stmt.setString(4, C_VALUE);
                stmt.setInt(5, 6);
                stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 1));
                stmt.setBigDecimal(7, null);
                stmt.setNull(8, Types.BIGINT);
                stmt.setNull(9, Types.INTEGER);
                stmt.setNull(10, Types.INTEGER);
                stmt.execute();

                stmt.setString(1, tenantId1);
                stmt.setString(2, ROW6);
                stmt.setString(3, B_VALUE);
                stmt.setString(4, C_VALUE);
                stmt.setInt(5, 5);
                stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 2));
                stmt.setBigDecimal(7, null);
                stmt.setNull(8, Types.BIGINT);
                stmt.setNull(9, Types.INTEGER);
                stmt.setNull(10, Types.INTEGER);
                stmt.execute();

                stmt.setString(1, tenantId1);
                stmt.setString(2, ROW7);
                stmt.setString(3, B_VALUE);
                stmt.setString(4, C_VALUE);
                stmt.setInt(5, 4);
                stmt.setDate(6, date == null ? null : date);
                stmt.setBigDecimal(7, BigDecimal.valueOf(0.1));
                stmt.setLong(8, 5L);
                stmt.setInt(9, 5);
                stmt.setNull(10, Types.INTEGER);
                stmt.execute();

                stmt.setString(1, tenantId1);
                stmt.setString(2, ROW8);
                stmt.setString(3, B_VALUE);
                stmt.setString(4, C_VALUE);
                stmt.setInt(5, 9);
                stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 1));
                stmt.setBigDecimal(7, BigDecimal.valueOf(3.9));
                long l = Integer.MIN_VALUE - 1L;
                assert (l < Integer.MIN_VALUE);
                stmt.setLong(8, l);
                stmt.setInt(9, 4);
                stmt.setNull(10, Types.INTEGER);
                stmt.execute();

                stmt.setString(1, tenantId1);
                stmt.setString(2, ROW9);
                stmt.setString(3, C_VALUE);
                stmt.setString(4, C_VALUE);
                stmt.setInt(5, 8);
                stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 2));
                stmt.setBigDecimal(7, BigDecimal.valueOf(3.3));
                l = Integer.MAX_VALUE + 1L;
                assert (l > Integer.MAX_VALUE);
                stmt.setLong(8, l);
                stmt.setInt(9, 3);
                stmt.setInt(10, 300);
                stmt.execute();
            }
            if (tenantId2 != null) {
                stmt.setString(1, tenantId2);
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

                stmt.setString(1, tenantId2);
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
            }
            conn.commit();
        } finally {
            conn.close();
        }
    }
}
