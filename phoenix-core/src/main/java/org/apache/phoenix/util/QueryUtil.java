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

package org.apache.phoenix.util;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

public class QueryUtil {
	
    /**
     *  Column family name index within ResultSet resulting from {@link DatabaseMetaData#getColumns(String, String, String, String)}
     */
    public static final int COLUMN_FAMILY_POSITION = 1;

 	/**
	 *  Column name index within ResultSet resulting from {@link DatabaseMetaData#getColumns(String, String, String, String)}
	 */
	public static final int COLUMN_NAME_POSITION = 4;
	/**
	 * Data type index within ResultSet resulting from {@link DatabaseMetaData#getColumns(String, String, String, String)}
	 */
	public static final int DATA_TYPE_POSITION = 5;

	/**
	 * Generates the upsert statement based on number of ColumnInfo. If
	 * ColumnInfo is unavailable, it produces a generic UPSERT query without
	 * columns information using number of columns.
	 * 
	 * @return Upsert Statement
	 */
	public static String constructUpsertStatement(ColumnInfo[] columnTypes,
			String tableName, int numColumns) {
		if(numColumns <= 0) {
			throw new RuntimeException("Number of columns in HBase table cannot be less than 1");
		}
		StringBuilder sb = new StringBuilder();
		sb.append("UPSERT INTO ");
		sb.append(tableName);
		if (columnTypes != null) {
			sb.append("(");
			for (ColumnInfo columnType : columnTypes) {
				if (columnType != null) {
					sb.append(columnType.getColumnName());
					sb.append(",");
				}
			}
			// Remove the trailing comma
			sb.setLength(sb.length() - 1);
			sb.append(") ");
		}
		sb.append("\n");
		sb.append("VALUES (");
		for (short i = 0; i < numColumns - 1; i++) {
			sb.append("?,");
		}
		sb.append("?)");

		return sb.toString();
	}

	public static String getUrl(String server) {
		return PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + server;
	}

    public static String getExplainPlan(ResultSet rs) throws SQLException {
        StringBuilder buf = new StringBuilder();
        while (rs.next()) {
            buf.append(rs.getString(1));
            buf.append('\n');
        }
        if (buf.length() > 0) {
            buf.setLength(buf.length()-1);
        }
        return buf.toString();
    }
}
