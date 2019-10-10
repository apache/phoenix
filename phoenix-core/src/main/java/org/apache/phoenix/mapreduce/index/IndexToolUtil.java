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
package org.apache.phoenix.mapreduce.index;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Utility class for {@linkplain IndexTool}
 *
 */
public class IndexToolUtil {

	private static final String ALTER_INDEX_QUERY_TEMPLATE = "ALTER INDEX IF EXISTS %s ON %s %s";  
    
	private static final Logger LOGGER = LoggerFactory.getLogger(IndexToolUtil.class);
	
	/**
	 * Updates the index state.
	 * @param configuration
	 * @param state
	 * @throws SQLException 
	 */
	public static void updateIndexState(Configuration configuration,PIndexState state) throws SQLException {
		final String masterTable = PhoenixConfigurationUtil.getInputTableName(configuration);
		final String[] indexTables = PhoenixConfigurationUtil.getDisableIndexes(configuration).split(",");
		final Properties overrideProps = new Properties();
		final Connection connection = ConnectionUtil.getOutputConnection(configuration, overrideProps);
		try {
            for (String indexTable : indexTables) {
                updateIndexState(connection, masterTable, indexTable, state);
            }
		} finally {
			if(connection != null) {
				connection.close();
			}
		}
	}

	/**
	 * Sets index state to Active.
	 * @param configuration
	 * @throws SQLException
	 */
	public static void setIndexToActive(Configuration configuration, String indexTable, String dataTable) throws SQLException {
		final Properties overrideProps = new Properties();
        final Connection connection = ConnectionUtil.getOutputConnection(configuration, overrideProps);
        try {
        	if (Strings.isNullOrEmpty(indexTable)) {
				indexTable = PhoenixConfigurationUtil.getIndexToolIndexTableName(configuration);
				if (Strings.isNullOrEmpty(indexTable)) {
					String schemaName = SchemaUtil.getSchemaNameFromFullName(
							PhoenixConfigurationUtil.getPhysicalTableName(configuration));
					indexTable = SchemaUtil.getTableName(schemaName, PhoenixConfigurationUtil.getDisableIndexes(configuration));
					// This one doesn't return schema name
				}
			}
			PTable indexPTable = PhoenixRuntime.getTable(connection, indexTable);
        	if (indexPTable != null) {
				// We have to mark it Building before we can set it to Active. Otherwise it errors out with
				// index state transition error
				if (indexPTable.getIndexState() == PIndexState.DISABLE) {
					IndexUtil.updateIndexState(connection.unwrap(PhoenixConnection.class),
							indexTable, PIndexState.BUILDING, null);
				}
			}

			if (Strings.isNullOrEmpty(dataTable)) {
				IndexToolUtil.updateIndexState(configuration, PIndexState.ACTIVE);
			} else {
				String justIndexName = SchemaUtil.getTableNameFromFullName(indexTable);
				IndexToolUtil.updateIndexState(connection, dataTable, justIndexName, PIndexState.ACTIVE);
			}
		} finally {
            if(connection != null) {
                connection.close();
            }
        }
    }
	
	/**
     * Updates the index state.
     * @param connection
     * @param masterTable
     * @param indexTable
     * @param state
     * @throws SQLException
     */
    public static void updateIndexState(Connection connection, final String masterTable , final String indexTable, PIndexState state) throws SQLException {
        Preconditions.checkNotNull(connection);
        final String alterQuery = String.format(ALTER_INDEX_QUERY_TEMPLATE,indexTable,masterTable,state.name());
        connection.createStatement().execute(alterQuery);
        LOGGER.info(" Updated the status of the index {} to {} " , indexTable , state.name());
    }
	
}
