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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PUnsignedLong;
import org.junit.Test;


public class DecodeFunctionIT extends BaseHBaseManagedTimeIT {

	@Test
	public void shouldPass() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE test_table ( some_column BINARY(12) NOT NULL CONSTRAINT PK PRIMARY KEY (some_column))";

		conn.createStatement().execute(ddl);
		PreparedStatement ps = conn.prepareStatement("UPSERT INTO test_table (some_column) VALUES (?)");

		byte[] kk = Bytes.add(PUnsignedLong.INSTANCE.toBytes(2232594215l), PInteger.INSTANCE.toBytes(-8));
		ps.setBytes(1, kk);

		ps.execute();
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM test_table WHERE some_column = DECODE('000000008512af277ffffff8', 'hex')");
		assertTrue(rs.next());
	}

	@Test
	public void upperCaseHexEncoding() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE test_table ( some_column BINARY(12) NOT NULL CONSTRAINT PK PRIMARY KEY (some_column))";

		conn.createStatement().execute(ddl);
		PreparedStatement ps = conn.prepareStatement("UPSERT INTO test_table (some_column) VALUES (?)");

		byte[] kk = Bytes.add(PUnsignedLong.INSTANCE.toBytes(2232594215l), PInteger.INSTANCE.toBytes(-8));
		ps.setBytes(1, kk);

		ps.execute();
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM test_table WHERE some_column = DECODE('000000008512af277ffffff8', 'HEX')");
		assertTrue(rs.next());
	}

	@Test
	public void invalidCharacters() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE test_table ( some_column BINARY(12) NOT NULL CONSTRAINT PK PRIMARY KEY (some_column))";

		conn.createStatement().execute(ddl);

		try {
			conn.createStatement().executeQuery("SELECT * FROM test_table WHERE some_column = DECODE('zzxxuuyyzzxxuuyy', 'hex')");
	        fail();
		} catch (SQLException e) {
			assertEquals(SQLExceptionCode.ILLEGAL_DATA.getErrorCode(), e.getErrorCode());
		}
	}

	@Test
	public void invalidLength() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE test_table ( some_column BINARY(12) NOT NULL CONSTRAINT PK PRIMARY KEY (some_column))";

		conn.createStatement().execute(ddl);

		try {
			conn.createStatement().executeQuery("SELECT * FROM test_table WHERE some_column = DECODE('8', 'hex')");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.ILLEGAL_DATA.getErrorCode(), e.getErrorCode());
        }
	}

	@Test
	public void nullEncoding() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE test_table ( some_column BINARY(12) NOT NULL CONSTRAINT PK PRIMARY KEY (some_column))";

		conn.createStatement().execute(ddl);

		try {
			conn.createStatement().executeQuery("SELECT * FROM test_table WHERE some_column = DECODE('8', NULL)");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.ILLEGAL_DATA.getErrorCode(), e.getErrorCode());
        }
	}

	@Test
	public void invalidEncoding() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE test_table ( some_column BINARY(12) NOT NULL CONSTRAINT PK PRIMARY KEY (some_column))";

		conn.createStatement().execute(ddl);

		try {
			conn.createStatement().executeQuery("SELECT * FROM test_table WHERE some_column = DECODE('8', 'someNonexistFormat')");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        }
	}
}
