/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.phoenix.pherf.util;

import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.configuration.Column;
import org.apache.phoenix.pherf.configuration.DataTypeMapping;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO This class needs to be cleanup up a bit. I just wanted to get an initial placeholder in.
public class PhoenixUtil {
	private static final Logger logger = LoggerFactory.getLogger(PhoenixUtil.class);
	private static String zookeeper;
	private static int rowCountOverride = 0;
	
    public Connection getConnection() throws Exception{
    	return getConnection(null);
    }
	
    public Connection getConnection(String tenantId) throws Exception{
		if (null == zookeeper) {
			throw new IllegalArgumentException("Zookeeper must be set before initializing connection!");
		}
    	Properties props = new Properties();
    	if (null != tenantId) {
    		props.setProperty("TenantId", tenantId);
   			logger.debug("\nSetting tenantId to " + tenantId);
    	}
    	Connection connection = DriverManager.getConnection("jdbc:phoenix:" + zookeeper, props);
        return connection;
    }

    public static void writeSfdcClientProperty() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		Map<String, String> sfdcProperty = conf.getValByRegex("sfdc");
    	Properties props = new Properties();
		for (Map.Entry<String, String> entry : sfdcProperty.entrySet()) {
			props.put(entry.getKey(), entry.getValue());
			logger.debug("\nSetting sfdc connection property " + entry.getKey() + " to " + entry.getValue());
		}
        OutputStream out = new java.io.FileOutputStream(new File("sfdc-hbase-client.properties"));
        props.store(out,"client properties");
    }
 
    public boolean executeStatement(String sql) throws Exception {
        Connection connection = null;
        boolean result = false;
        try {
            connection = getConnection();
            result = executeStatement(sql, connection);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
        return result;
    }
    
    public boolean executeStatement(String sql, Connection connection) {
    	boolean result = false;
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            result = preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }
    
    public boolean executeStatement(PreparedStatement preparedStatement, Connection connection) {
    	boolean result = false;
        try {
            result = preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public ResultSet executeQuery(PreparedStatement preparedStatement, Connection connection) {
        ResultSet resultSet = null;
        try {
            resultSet = preparedStatement.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultSet;
    }
    
    /**
     * Delete existing tables with schema name set as {@link PherfConstants#PHERF_SCHEMA_NAME} with regex comparison 
     * 
     * @param regexMatch
     * @throws SQLException
     * @throws Exception
     */
    public void deleteTables(String regexMatch) throws SQLException, Exception {
    	regexMatch = regexMatch.toUpperCase().replace("ALL", ".*");
    	Connection conn = getConnection();
    	try {
        	ResultSet resultSet = getTableMetaData(PherfConstants.PHERF_SCHEMA_NAME, null, conn);
	    	while (resultSet.next()) {
	    		String tableName = resultSet.getString("TABLE_SCHEM") == null ? resultSet.getString("TABLE_NAME") : 
	    						   resultSet.getString("TABLE_SCHEM") + "." + resultSet.getString("TABLE_NAME");
	    		if (tableName.matches(regexMatch)) {
		    		logger.info("\nDropping " + tableName);
		    		executeStatement("DROP TABLE " + tableName + " CASCADE", conn);
	    		}
	    	}
    	} finally {
    		conn.close();
    	}
    }
    
    public ResultSet getTableMetaData(String schemaName, String tableName, Connection connection) throws SQLException {
    	DatabaseMetaData dbmd = connection.getMetaData();
    	ResultSet resultSet = dbmd.getTables(null, schemaName, tableName, null);
    	return resultSet;
    }
    
    public ResultSet getColumnsMetaData(String schemaName, String tableName, Connection connection) throws SQLException {
    	DatabaseMetaData dbmd = connection.getMetaData();
    	ResultSet resultSet = dbmd.getColumns(null, schemaName, tableName, null);
    	return resultSet;
    }
    
    public synchronized List<Column> getColumnsFromPhoenix(String schemaName, String tableName, Connection connection) throws SQLException {
    	List<Column> columnList = new ArrayList<Column>();
    	ResultSet resultSet = null;
    	try {
    		resultSet = getColumnsMetaData(schemaName, tableName, connection);
    		while (resultSet.next()) {
    			Column column = new Column();
    	        column.setName(resultSet.getString("COLUMN_NAME"));
    	        column.setType(DataTypeMapping.valueOf(resultSet.getString("TYPE_NAME")));
    	        column.setLength(resultSet.getInt("COLUMN_SIZE"));
    	        columnList.add(column);
   	        }
    	} finally {
    		if (null != resultSet) { 
    			resultSet.close();
    		}
    	}
    	
    	return Collections.unmodifiableList(columnList);
    }
    
	public static String getZookeeper() {
		return zookeeper;
	}

	public static void setZookeeper(String zookeeper) {
		logger.info("Setting zookeeper: " + zookeeper);
		PhoenixUtil.zookeeper = zookeeper;
	}
	
	public static int getRowCountOverride() {
		return rowCountOverride;
	}
	
	public static void setRowCountOverride(int rowCountOverride) {
		PhoenixUtil.rowCountOverride = rowCountOverride;
	}
}
