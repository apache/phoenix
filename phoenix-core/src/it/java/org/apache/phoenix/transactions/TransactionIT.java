/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.transactions;

import static org.apache.phoenix.util.TestUtil.INDEX_DATA_SCHEMA;
import static org.apache.phoenix.util.TestUtil.TRANSACTIONAL_DATA_TABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import co.cask.tephra.TxConstants;

import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.end2end.Shadower;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class TransactionIT extends BaseHBaseManagedTimeIT {
	
	private static final String FULL_TABLE_NAME = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + TRANSACTIONAL_DATA_TABLE;
	
    @Before
    public void setUp() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute(
                  "create table "+ FULL_TABLE_NAME + "("
                + "   varchar_pk VARCHAR NOT NULL, "
                + "   char_pk CHAR(6) NOT NULL, "
                + "   int_pk INTEGER NOT NULL, "
                + "   long_pk BIGINT NOT NULL, "
                + "   decimal_pk DECIMAL(31, 10) NOT NULL, "
                + "   date_pk DATE NOT NULL, "
                + "   a.varchar_col1 VARCHAR, "
                + "   a.char_col1 CHAR(10), "
                + "   a.int_col1 INTEGER, "
                + "   a.long_col1 BIGINT, "
                + "   a.decimal_col1 DECIMAL(31, 10), "
                + "   a.date1 DATE, "
                + "   b.varchar_col2 VARCHAR, "
                + "   b.char_col2 CHAR(10), "
                + "   b.int_col2 INTEGER, "
                + "   b.long_col2 BIGINT, "
                + "   b.decimal_col2 DECIMAL(31, 10), "
                + "   b.date2 DATE "
                + "   CONSTRAINT pk PRIMARY KEY (varchar_pk, char_pk, int_pk, long_pk DESC, decimal_pk, date_pk)) "
                + "TRANSACTIONAL=true");
        } finally {
            conn.close();
        }
    }

	@BeforeClass
    @Shadower(classBeingShadowed = BaseHBaseManagedTimeIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        props.put(TxConstants.ALLOW_EMPTY_VALUES_KEY, Boolean.toString(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
	
	private void setRowKeyColumns(PreparedStatement stmt, int i) throws SQLException {
        // insert row
        stmt.setString(1, "varchar" + String.valueOf(i));
        stmt.setString(2, "char" + String.valueOf(i));
        stmt.setInt(3, i);
        stmt.setLong(4, i);
        stmt.setBigDecimal(5, new BigDecimal(i*0.5d));
        Date date = new Date(DateUtil.parseDate("2015-01-01 00:00:00").getTime() + (i - 1) * TestUtil.NUM_MILLIS_IN_DAY);
        stmt.setDate(6, date);
    }
	
	private void validateRowKeyColumns(ResultSet rs, int i) throws SQLException {
		assertTrue(rs.next());
		assertEquals(rs.getString(1), "varchar" + String.valueOf(i));
		assertEquals(rs.getString(2), "char" + String.valueOf(i));
		assertEquals(rs.getInt(3), i);
		assertEquals(rs.getInt(4), i);
		assertEquals(rs.getBigDecimal(5), new BigDecimal(i*0.5d));
		Date date = new Date(DateUtil.parseDate("2015-01-01 00:00:00").getTime() + (i - 1) * TestUtil.NUM_MILLIS_IN_DAY);
		assertEquals(rs.getDate(6), date);
	}
	
	@Test
	public void testReadOwnWrites() throws Exception {
		String selectSql = "SELECT * FROM "+FULL_TABLE_NAME;
		Connection conn = DriverManager.getConnection(getUrl());
		try {
			conn.setAutoCommit(false);
			ResultSet rs = conn.createStatement().executeQuery(selectSql);
	     	assertFalse(rs.next());
	     	
	        String upsert = "UPSERT INTO " + FULL_TABLE_NAME + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk) VALUES(?, ?, ?, ?, ?, ?)";
	        PreparedStatement stmt = conn.prepareStatement(upsert);
			// upsert two rows
			setRowKeyColumns(stmt, 1);
			stmt.execute();
			setRowKeyColumns(stmt, 2);
			stmt.execute();
	        
	        // verify rows can be read even though commit has not been called
			rs = conn.createStatement().executeQuery(selectSql);
			validateRowKeyColumns(rs, 1);
	        validateRowKeyColumns(rs, 2);
	        assertFalse(rs.next());
	        
	        conn.commit();
	        
	        // verify rows can be read after commit
	        rs = conn.createStatement().executeQuery(selectSql);
	        validateRowKeyColumns(rs, 1);
	        validateRowKeyColumns(rs, 2);
	        assertFalse(rs.next());
		}
        finally {
        	conn.close();
        }
	}
	
	@Test
	public void testAutoCommitQuerySingleTable() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		try {
			conn.setAutoCommit(true);
			// verify no rows returned
			ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + FULL_TABLE_NAME);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}
	
    @Test
    public void testAutoCommitQueryMultiTables() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.setAutoCommit(true);
            // verify no rows returned
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + FULL_TABLE_NAME + " a JOIN " + FULL_TABLE_NAME + " b ON (a.long_pk = b.int_pk)");
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
	@Test
	public void testColConflicts() throws Exception {
		Connection conn1 = DriverManager.getConnection(getUrl());
		Connection conn2 = DriverManager.getConnection(getUrl());
		try {
			conn1.setAutoCommit(false);
			conn2.setAutoCommit(false);
			String selectSql = "SELECT * FROM "+FULL_TABLE_NAME;
			conn1.setAutoCommit(false);
			ResultSet rs = conn1.createStatement().executeQuery(selectSql);
	     	assertFalse(rs.next());
			// upsert row using conn1
			String upsertSql = "UPSERT INTO " + FULL_TABLE_NAME + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk, a.int_col1) VALUES(?, ?, ?, ?, ?, ?, ?)";
			PreparedStatement stmt = conn1.prepareStatement(upsertSql);
			setRowKeyColumns(stmt, 1);
			stmt.setInt(7, 10);
	        stmt.execute();
	        // upsert row using conn2
 			stmt = conn2.prepareStatement(upsertSql);
 			setRowKeyColumns(stmt, 1);
			stmt.setInt(7, 11);
	        stmt.execute();
 	        
 	        conn1.commit();
	        //second commit should fail
 	        try {
 	 	        conn2.commit();
 	 	        fail();
 	        }	
 	        catch (SQLException e) {
 	        	assertEquals(e.getErrorCode(), SQLExceptionCode.TRANSACTION_CONFLICT_EXCEPTION.getErrorCode());
 	        }
		}
        finally {
        	conn1.close();
        }
	}
	
	@Test
	public void testRowConflicts() throws Exception {
		Connection conn1 = DriverManager.getConnection(getUrl());
		Connection conn2 = DriverManager.getConnection(getUrl());
		try {
			conn1.setAutoCommit(false);
			conn2.setAutoCommit(false);
			String selectSql = "SELECT * FROM "+FULL_TABLE_NAME;
			conn1.setAutoCommit(false);
			ResultSet rs = conn1.createStatement().executeQuery(selectSql);
	     	assertFalse(rs.next());
			// upsert row using conn1
			String upsertSql = "UPSERT INTO " + FULL_TABLE_NAME + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk, a.int_col1) VALUES(?, ?, ?, ?, ?, ?, ?)";
			PreparedStatement stmt = conn1.prepareStatement(upsertSql);
			setRowKeyColumns(stmt, 1);
			stmt.setInt(7, 10);
	        stmt.execute();
	        // upsert row using conn2
	        upsertSql = "UPSERT INTO " + FULL_TABLE_NAME + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk, b.int_col2) VALUES(?, ?, ?, ?, ?, ?, ?)";
 			stmt = conn2.prepareStatement(upsertSql);
 			setRowKeyColumns(stmt, 1);
			stmt.setInt(7, 11);
 	        stmt.execute();
 	        
 	        conn1.commit();
	        //second commit should fail
 	        try {
 	 	        conn2.commit();
 	 	        fail();
 	        }	
 	        catch (SQLException e) {
 	        	assertEquals(e.getErrorCode(), SQLExceptionCode.TRANSACTION_CONFLICT_EXCEPTION.getErrorCode());
 	        }
		}
        finally {
        	conn1.close();
        }
	}



}
