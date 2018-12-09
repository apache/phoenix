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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

public class ExpressionCompilerIT extends BaseHBaseManagedTimeIT {
	@SuppressWarnings("deprecation")
	@Test
	public void testLikeOperator() throws SQLException {
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		Connection conn = DriverManager.getConnection(getUrl(), props);
		conn.setAutoCommit(false);

		try {
			String ddl = "CREATE TABLE test_table "
					+ "  (a_string varchar not null primary key)";
			createTestTable(getUrl(), ddl);

			String upsertQuery = "UPSERT INTO test_table VALUES('Example')";
			PreparedStatement stmt = conn.prepareStatement(upsertQuery);
			stmt.execute();

			for (PDataType<?> dataType : PDataType.values()) {
				if (dataType.isArrayType() || PVarbinary.INSTANCE == dataType
						|| PBinary.INSTANCE == dataType) {
					continue;
				}
				Object value1 = dataType.getSampleValue();
				String selectQuery = "select * from test_table where ?"
						+ " LIKE " + "?";
				stmt = conn.prepareStatement(selectQuery);
				stmt.setObject(1, value1, dataType.getSqlType());
				stmt.setObject(2, value1, dataType.getSqlType());
				try {
					stmt.execute();
					if (PVarchar.INSTANCE != dataType
							&& PChar.INSTANCE != dataType) {
						fail("LIKE with type " + dataType
								+ " should throw exception.");
					}
				} catch (SQLException sqe) {
					assertEquals(
							SQLExceptionCode.TYPE_NOT_SUPPORTED_FOR_OPERATOR
									.getErrorCode(),
							sqe.getErrorCode());
				}
			}

		} finally {
			conn.close();
		}
	}
}
