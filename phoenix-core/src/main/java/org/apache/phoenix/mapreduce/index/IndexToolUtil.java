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

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.schema.PIndexState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Utility class for {@linkplain IndexTool}
 *
 */
public class IndexToolUtil {

	private static final String ALTER_INDEX_QUERY_TEMPLATE = "ALTER INDEX IF EXISTS %s ON %s %s";  
    
	private static final Logger LOG = LoggerFactory.getLogger(IndexToolUtil.class);
	
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
        LOG.info(" Updated the status of the index {} to {} " , indexTable , state.name());
    }
	
}
