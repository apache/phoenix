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

import static org.apache.commons.lang.StringUtils.isNotEmpty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.pig.util.ColumnInfoToStringEncoderDecoder;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;


/**
 * A container for configuration to be used with {@link PhoenixHBaseStorage} and {@link PhoenixHBaseLoader}
 * 
 */
public class PhoenixPigConfiguration {
	
	private static final Log LOG = LogFactory.getLog(PhoenixPigConfiguration.class);
	
	private PhoenixPigConfigurationUtil util;
	
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
	
	public static final String UPSERT_COLUMN_INFO_KEY  = "phoenix.upsert.columninfos.list";
	
	public static final String SELECT_STATEMENT = "phoenix.select.stmt";
	
	public static final String UPSERT_BATCH_SIZE = "phoenix.upsert.batch.size";
	
	//columns projected given as part of LOAD.
	public static final String SELECT_COLUMNS = "phoneix.select.query.columns";
	
	public static final String SELECT_COLUMN_INFO_KEY  = "phoenix.select.columninfos.list";
	
	// the delimiter supported during LOAD and STORE when projected columns are given.
	public static final String COLUMN_NAMES_DELIMITER = "phoenix.column.names.delimiter";
	
	public static final long DEFAULT_UPSERT_BATCH_SIZE = 1000;
	
	public static final String DEFAULT_COLUMN_NAMES_DELIMITER = ",";
	
	private final Configuration conf;
		
	public PhoenixPigConfiguration(Configuration conf) {
		this.conf = conf;
		this.util = new PhoenixPigConfigurationUtil();
	}
	
	public void configure(String server, String tableName, long batchSize) {
        configure(server,tableName,batchSize,null);
    }
	
	public void configure(String server, String tableName, long batchSize, String columns) {
	    conf.set(SERVER_NAME, server);
        conf.set(TABLE_NAME, tableName);
        conf.setLong(UPSERT_BATCH_SIZE, batchSize);
        if (isNotEmpty(columns)) {
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
	    return getUtil().getConnection(getConfiguration());
	}
	
	public String getUpsertStatement() throws SQLException {
		return getUtil().getUpsertStatement(getConfiguration(), getTableName());
	}

	public long getBatchSize() throws SQLException {
		return getUtil().getBatchSize(getConfiguration());
	}

	public String getServer() {
		return conf.get(SERVER_NAME);
	}

	public List<ColumnInfo> getColumnMetadataList() throws SQLException {
	    return getUtil().getUpsertColumnMetadataList(getConfiguration(), getTableName());
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
	
	public String getSelectStatement() throws SQLException {
	   return getUtil().getSelectStatement(getConfiguration(), getTableName());
	}
	
	public List<ColumnInfo> getSelectColumnMetadataList() throws SQLException {
        return getUtil().getSelectColumnMetadataList(getConfiguration(), getTableName());
    }
	
	public void setServerName(final String zookeeperQuorum) {
	    this.conf.set(SERVER_NAME, zookeeperQuorum);
	}
	
	public void setTableName(final String tableName) {
	    Preconditions.checkNotNull(tableName, "HBase Table name cannot be null!");
	    this.conf.set(TABLE_NAME, tableName);
	}
	
	public void setSelectStatement(final String selectStatement) {
	    this.conf.set(SELECT_STATEMENT, selectStatement);
	}

	public void setSelectColumns(String selectColumns) {
        this.conf.set(SELECT_COLUMNS, selectColumns);
    }
	
	public PhoenixPigConfigurationUtil getUtil() {
	    return this.util;
	}
	
		
	@VisibleForTesting
	static class PhoenixPigConfigurationUtil {
                
        public Connection getConnection(final Configuration configuration) throws SQLException {
            Preconditions.checkNotNull(configuration);
            Properties props = new Properties();
            final Connection conn = DriverManager.getConnection(QueryUtil.getUrl(configuration.get(SERVER_NAME)), props).unwrap(PhoenixConnection.class);
            conn.setAutoCommit(false);
            return conn;
        }
        
        public List<ColumnInfo> getUpsertColumnMetadataList(final Configuration configuration,final String tableName) throws SQLException {
            Preconditions.checkNotNull(configuration);
            Preconditions.checkNotNull(tableName);
            final String columnInfoStr = configuration.get(UPSERT_COLUMN_INFO_KEY);
            if(isNotEmpty(columnInfoStr)) {
                return ColumnInfoToStringEncoderDecoder.decode(columnInfoStr);
            }
            final Connection connection = getConnection(configuration);
            String upsertColumns = configuration.get(UPSERT_COLUMNS);
            List<String> upsertColumnList = null;
            if(isNotEmpty(upsertColumns)) {
                final String columnNamesDelimiter = configuration.get(COLUMN_NAMES_DELIMITER, DEFAULT_COLUMN_NAMES_DELIMITER);
                upsertColumnList = Lists.newArrayList(Splitter.on(columnNamesDelimiter).omitEmptyStrings().trimResults().split(upsertColumns));
                LOG.info(String.format("UseUpsertColumns=%s, upsertColumns=%s, upsertColumnSet.size()=%s, parsedColumns=%s "
                        ,!upsertColumnList.isEmpty(),upsertColumns, upsertColumnList.size(), Joiner.on(DEFAULT_COLUMN_NAMES_DELIMITER).join(upsertColumnList)
                        ));
            } 
           List<ColumnInfo> columnMetadataList = PhoenixRuntime.generateColumnInfo(connection, tableName, upsertColumnList);
           final String encodedColumnInfos = ColumnInfoToStringEncoderDecoder.encode(columnMetadataList);
           // we put the encoded column infos in the Configuration for re usability. 
           configuration.set(UPSERT_COLUMN_INFO_KEY, encodedColumnInfos);
           closeConnection(connection);
           return columnMetadataList;
        }
        
        public String getUpsertStatement(final Configuration configuration,final String tableName) throws SQLException {
            Preconditions.checkNotNull(configuration);
            Preconditions.checkNotNull(tableName);
            String upsertStmt = configuration.get(UPSERT_STATEMENT);
            if(isNotEmpty(upsertStmt)) {
                return upsertStmt;
            }
            final boolean useUpsertColumns = isNotEmpty(configuration.get(UPSERT_COLUMNS,""));
            final List<ColumnInfo> columnMetadataList = getUpsertColumnMetadataList(configuration, tableName);
            if (useUpsertColumns) {
                // Generating UPSERT statement without column name information.
                upsertStmt = QueryUtil.constructUpsertStatement(tableName, columnMetadataList);
                LOG.info("Phoenix Custom Upsert Statement: "+ upsertStmt);
            } else {
                // Generating UPSERT statement without column name information.
                upsertStmt = QueryUtil.constructGenericUpsertStatement(tableName, columnMetadataList.size());
                LOG.info("Phoenix Generic Upsert Statement: " + upsertStmt);
            }
            configuration.set(UPSERT_STATEMENT, upsertStmt);
            return upsertStmt;
            
        }
        
        public List<ColumnInfo> getSelectColumnMetadataList(final Configuration configuration,final String tableName) throws SQLException {
            Preconditions.checkNotNull(configuration);
            Preconditions.checkNotNull(tableName);
            final String columnInfoStr = configuration.get(SELECT_COLUMN_INFO_KEY);
            if(isNotEmpty(columnInfoStr)) {
                return ColumnInfoToStringEncoderDecoder.decode(columnInfoStr);
            }
            final Connection connection = getConnection(configuration);
            String selectColumns = configuration.get(SELECT_COLUMNS);
            List<String> selectColumnList = null;
            if(isNotEmpty(selectColumns)) {
                final String columnNamesDelimiter = configuration.get(COLUMN_NAMES_DELIMITER, DEFAULT_COLUMN_NAMES_DELIMITER);
                selectColumnList = Lists.newArrayList(Splitter.on(columnNamesDelimiter).omitEmptyStrings().trimResults().split(selectColumns));
                LOG.info(String.format("UseSelectColumns=%s, selectColumns=%s, selectColumnSet.size()=%s, parsedColumns=%s "
                        ,!selectColumnList.isEmpty(),selectColumns, selectColumnList.size(), Joiner.on(DEFAULT_COLUMN_NAMES_DELIMITER).join(selectColumnList)
                        ));
            }
           List<ColumnInfo> columnMetadataList = PhoenixRuntime.generateColumnInfo(connection, tableName, selectColumnList);
           final String encodedColumnInfos = ColumnInfoToStringEncoderDecoder.encode(columnMetadataList);
           // we put the encoded column infos in the Configuration for re usability. 
           configuration.set(SELECT_COLUMN_INFO_KEY, encodedColumnInfos);
           closeConnection(connection);
           return columnMetadataList;
        }
        
        public String getSelectStatement(final Configuration configuration,final String tableName) throws SQLException {
            Preconditions.checkNotNull(configuration);
            Preconditions.checkNotNull(tableName);
            String selectStmt = configuration.get(SELECT_STATEMENT);
            if(isNotEmpty(selectStmt)) {
                return selectStmt;
            }
            final List<ColumnInfo> columnMetadataList = getSelectColumnMetadataList(configuration, tableName);
            selectStmt = QueryUtil.constructSelectStatement(tableName, columnMetadataList);
            LOG.info("Select Statement: "+ selectStmt);
            configuration.set(SELECT_STATEMENT, selectStmt);
            return selectStmt;
        }
        
        public long getBatchSize(final Configuration configuration) throws SQLException {
            Preconditions.checkNotNull(configuration);
            long batchSize = configuration.getLong(UPSERT_BATCH_SIZE, DEFAULT_UPSERT_BATCH_SIZE);
            if(batchSize <= 0) {
               Connection conn = getConnection(configuration);
               batchSize = ((PhoenixConnection) conn).getMutateBatchSize();
               closeConnection(conn);
            }
            configuration.setLong(UPSERT_BATCH_SIZE, batchSize);
            return batchSize;
        }
        
        private void closeConnection(final Connection connection) throws SQLException {
            if(connection != null) {
                connection.close();
            }
        }
    }
    
}
