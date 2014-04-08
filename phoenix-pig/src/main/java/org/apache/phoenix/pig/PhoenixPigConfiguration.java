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

package org.apache.phoenix.pig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;

import com.google.common.collect.Lists;

/**
 * A container for configuration to be used with {@link PhoenixHBaseStorage}
 * 
 * 
 * 
 */
public class PhoenixPigConfiguration {
	
	private static final Log LOG = LogFactory.getLog(PhoenixPigConfiguration.class);
	
	/**
	 * Speculative execution of Map tasks
	 */
	public static final String MAP_SPECULATIVE_EXEC = "mapred.map.tasks.speculative.execution";

	/**
	 * Speculative execution of Reduce tasks
	 */
	public static final String REDUCE_SPECULATIVE_EXEC = "mapred.reduce.tasks.speculative.execution";
	
	public static final String SERVER_NAME = "phoenix.hbase.server.name";
	
	public static final String TABLE_NAME = "phoenix.hbase.table.name";
	
	public static final String UPSERT_COLUMNS = "phoenix.hbase.upsert.columns";
	
	public static final String UPSERT_STATEMENT = "phoenix.upsert.stmt";
	
	public static final String UPSERT_BATCH_SIZE = "phoenix.upsert.batch.size";
	
	public static final long DEFAULT_UPSERT_BATCH_SIZE = 1000;
	
	private final Configuration conf;
	
	private Connection conn;
	private List<ColumnInfo> columnMetadataList;
		
	public PhoenixPigConfiguration(Configuration conf) {
		this.conf = conf;
	}
	
	public void configure(String server, String tableName, long batchSize) {
		configure(server,tableName,batchSize,null);
	}
	
	public void configure(String server, String tableName, long batchSize, String columns) {
		conf.set(SERVER_NAME, server);
		conf.set(TABLE_NAME, tableName);
		conf.setLong(UPSERT_BATCH_SIZE, batchSize);
		if (columns!=null) {
			conf.set(UPSERT_COLUMNS, columns);
		}

		conf.setBoolean(MAP_SPECULATIVE_EXEC, false);
		conf.setBoolean(REDUCE_SPECULATIVE_EXEC, false);
	}
	
	/**
	 * Creates a {@link Connection} with autoCommit set to false.
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException {
		Properties props = new Properties();
		conn = DriverManager.getConnection(QueryUtil.getUrl(this.conf.get(SERVER_NAME)), props).unwrap(PhoenixConnection.class);
		conn.setAutoCommit(false);
		
		setup(conn);
		
		return conn;
	}
	
	/**
	 * This method creates the Upsert statement and the Column Metadata
	 * for the Pig query using {@link PhoenixHBaseStorage}. It also 
	 * determines the batch size based on user provided options.
	 * 
	 * @param conn
	 * @throws SQLException
	 */
	public void setup(Connection conn) throws SQLException {
		// Reset batch size
		long batchSize = getBatchSize() <= 0 ? ((PhoenixConnection) conn).getMutateBatchSize() : getBatchSize();
		conf.setLong(UPSERT_BATCH_SIZE, batchSize);
		boolean useUpsertColumns = false;
		
		if (columnMetadataList == null) {
			String upsertColumns = getUpsertColumns();
			List<String> upsertColumnList = Lists.newArrayList();
			if (StringUtils.isNotEmpty(upsertColumns)) {
				useUpsertColumns = true;
				String[] upsertColumnTokens = upsertColumns.split(",");
				for (String token : upsertColumnTokens) {
					upsertColumnList.add(token.trim());
				}
			}

			StringBuilder parsedColumns = new StringBuilder();
			boolean first = true;
			for (String key : upsertColumnList) {
				if (first) first=false;
				else parsedColumns.append(",");
				parsedColumns.append(key);
			}
			LOG.info(String.format("UseUpsertColumns=%s, upsertColumns=%s, upsertColumnSet.size()=%s, parsedColumns=%s "
					,useUpsertColumns,upsertColumns, upsertColumnList.size(), parsedColumns.toString()
					));

			columnMetadataList = PhoenixRuntime.generateColumnInfo(conn, getTableName(), upsertColumnList);
			
		}
		
		if (useUpsertColumns) {
			// Generating UPSERT statement without column name information.
			String upsertStmt = QueryUtil.constructUpsertStatement(getTableName(), columnMetadataList);
			LOG.info("Phoenix Custom Upsert Statement: " + upsertStmt);
			conf.set(UPSERT_STATEMENT, upsertStmt);
			
		} else {
			// Generating UPSERT statement without column name information.
			String upsertStmt = QueryUtil.constructGenericUpsertStatement(getTableName(), columnMetadataList.size());
			LOG.info("Phoenix Generic Upsert Statement: " + upsertStmt);
			conf.set(UPSERT_STATEMENT, upsertStmt);
		}
	}
	
    public String getUpsertStatement() {
		return conf.get(UPSERT_STATEMENT);
	}

	public long getBatchSize() {
		return conf.getLong(UPSERT_BATCH_SIZE, DEFAULT_UPSERT_BATCH_SIZE);
	}


	public String getServer() {
		return conf.get(SERVER_NAME);
	}

	public List<ColumnInfo> getColumnMetadataList() {
		return columnMetadataList;
	}
	
	public String getUpsertColumns() {
		return conf.get(UPSERT_COLUMNS);
	}
	
	public String getTableName() {
		return conf.get(TABLE_NAME);
	}

	public Configuration getConfiguration() {
		return this.conf;
	}

}
