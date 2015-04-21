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
package org.apache.phoenix.transactions;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.phoenix.end2end.BaseClientManagedTimeIT;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Before;
import org.junit.Test;

public class TxPointInTimeQueryIT extends BaseClientManagedTimeIT {

	protected long ts;

	@Before
	public void initTable() throws Exception {
		ts = nextTimestamp();
	}

	@Test
	public void testDropColumn() throws Exception {
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
		Connection conn = DriverManager.getConnection(getUrl(), props);
		try {
			conn.createStatement()
					.execute(
							"CREATE TABLE t (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR) TRANSACTIONAL=true");

			props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
			conn = DriverManager.getConnection(getUrl(), props);
			// drop a column
			conn.createStatement().execute("ALTER TABLE t DROP COLUMN v1");

			props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 11));
			conn = DriverManager.getConnection(getUrl(), props);
			String selectQuery = "SELECT v1 FROM t";
			try {
				conn.createStatement().executeQuery(selectQuery);
				fail();
			} catch (ColumnNotFoundException e) {
			}

			props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 5));
			conn = DriverManager.getConnection(getUrl(), props);
			ResultSet rs = conn.createStatement().executeQuery(selectQuery);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}

}
