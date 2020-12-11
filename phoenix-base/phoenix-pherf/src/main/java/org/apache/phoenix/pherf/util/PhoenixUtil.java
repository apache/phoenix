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

import org.apache.phoenix.mapreduce.index.automation.PhoenixMRJobSubmitter;
import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.configuration.*;
import org.apache.phoenix.pherf.result.DataLoadTimeSummary;
import org.apache.phoenix.pherf.rules.RulesApplier;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;

public class PhoenixUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixUtil.class);
    private static String zookeeper;
    private static int rowCountOverride = 0;
    private boolean testEnabled;
    private static PhoenixUtil instance;
    private static boolean useThinDriver;
    private static String queryServerUrl;
    private static final String ASYNC_KEYWORD = "ASYNC";
    private static final int ONE_MIN_IN_MS = 60000;
    private static String CurrentSCN = null;

    private PhoenixUtil() {
        this(false);
    }

    private PhoenixUtil(final boolean testEnabled) {
        this.testEnabled = testEnabled;
    }

    public static PhoenixUtil create() {
        return create(false);
    }

    public static PhoenixUtil create(final boolean testEnabled) {
        instance = instance != null ? instance : new PhoenixUtil(testEnabled);
        return instance;
    }

    public static void useThinDriver(String queryServerUrl) {
        PhoenixUtil.useThinDriver = true;
        PhoenixUtil.queryServerUrl = Objects.requireNonNull(queryServerUrl);
    }

    public static String getQueryServerUrl() {
        return PhoenixUtil.queryServerUrl;
    }

    public static boolean isThinDriver() {
        return PhoenixUtil.useThinDriver;
    }

    public Connection getConnection() throws Exception {
        return getConnection(null);
    }

    public Connection getConnection(String tenantId) throws Exception {
        return getConnection(tenantId, testEnabled, null);
    }
    
    public Connection getConnection(String tenantId, Map<String, String> phoenixProperty) throws Exception {
        return getConnection(tenantId, testEnabled, phoenixProperty);
    }

    public Connection getConnection(String tenantId, boolean testEnabled, Map<String, String> phoenixProperty) throws Exception {
        if (useThinDriver) {
            if (null == queryServerUrl) {
                throw new IllegalArgumentException("QueryServer URL must be set before" +
                      " initializing connection");
            }
            Properties props = new Properties();
            if (null != tenantId) {
                props.setProperty("TenantId", tenantId);
                LOGGER.debug("\nSetting tenantId to " + tenantId);
            }
            String url = "jdbc:phoenix:thin:url=" + queryServerUrl + ";serialization=PROTOBUF";
            return DriverManager.getConnection(url, props);
        } else {
            if (null == zookeeper) {
                throw new IllegalArgumentException(
                        "Zookeeper must be set before initializing connection!");
            }
            Properties props = new Properties();
            if (null != tenantId) {
                props.setProperty("TenantId", tenantId);
                LOGGER.debug("\nSetting tenantId to " + tenantId);
            }
            
            if (phoenixProperty != null) {
            	for (Map.Entry<String, String> phxProperty: phoenixProperty.entrySet()) {
            		props.setProperty(phxProperty.getKey(), phxProperty.getValue());
					System.out.println("Setting connection property "
							+ phxProperty.getKey() + " to "
							+ phxProperty.getValue());
            	}
            }
            
            String url = "jdbc:phoenix:" + zookeeper + (testEnabled ? ";test=true" : "");
            return DriverManager.getConnection(url, props);
        }
    }

    public boolean executeStatement(String sql, Scenario scenario) throws Exception {
        Connection connection = null;
        boolean result = false;
        try {
            connection = getConnection(scenario.getTenantId());
            result = executeStatement(sql, connection);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
        return result;
    }

    /**
     * Execute statement
     *
     * @param sql
     * @param connection
     * @return
     * @throws SQLException
     */
    public boolean executeStatementThrowException(String sql, Connection connection)
            throws SQLException {
        boolean result = false;
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            result = preparedStatement.execute();
            connection.commit();
        } finally {
            if(preparedStatement != null) {
                preparedStatement.close();
            }
        }
        return result;
    }

    public boolean executeStatement(String sql, Connection connection) throws SQLException{
        boolean result = false;
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            result = preparedStatement.execute();
            connection.commit();
        } finally {
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @SuppressWarnings("unused")
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

    /**
     * Delete existing tables with schema name set as {@link PherfConstants#PHERF_SCHEMA_NAME} with regex comparison
     *
     * @param regexMatch
     * @throws SQLException
     * @throws Exception
     */
    public void deleteTables(String regexMatch) throws Exception {
        regexMatch = regexMatch.toUpperCase().replace("ALL", ".*");
        Connection conn = getConnection();
        try {
            ResultSet resultSet = getTableMetaData(PherfConstants.PHERF_SCHEMA_NAME, null, conn);
            while (resultSet.next()) {
                String tableName = resultSet.getString(TABLE_SCHEM) == null ? resultSet
                        .getString(TABLE_NAME) : resultSet
                        .getString(TABLE_SCHEM)
                        + "."
                        + resultSet.getString(TABLE_NAME);
                if (tableName.matches(regexMatch)) {
                    LOGGER.info("\nDropping " + tableName);
                    try {
                        executeStatementThrowException("DROP TABLE "
                                + tableName + " CASCADE", conn);
                    } catch (org.apache.phoenix.schema.TableNotFoundException tnf) {
                        LOGGER.error("Table might be already be deleted via cascade. Schema: "
                                + tnf.getSchemaName()
                                + " Table: "
                                + tnf.getTableName());
                    }
                }
            }
        } finally {
            conn.close();
        }
    }

    public ResultSet getTableMetaData(String schemaName, String tableName, Connection connection)
            throws SQLException {
        DatabaseMetaData dbmd = connection.getMetaData();
        ResultSet resultSet = dbmd.getTables(null, schemaName, tableName, null);
        return resultSet;
    }

    public ResultSet getColumnsMetaData(String schemaName, String tableName, Connection connection)
            throws SQLException {
        DatabaseMetaData dbmd = connection.getMetaData();
        ResultSet resultSet = dbmd.getColumns(null, schemaName.toUpperCase(), tableName.toUpperCase(), null);
        return resultSet;
    }

    public synchronized List<Column> getColumnsFromPhoenix(String schemaName, String tableName,
            Connection connection) throws SQLException {
        List<Column> columnList = new ArrayList<>();
        ResultSet resultSet = null;
        try {
            resultSet = getColumnsMetaData(schemaName, tableName, connection);
            while (resultSet.next()) {
                Column column = new Column();
                column.setName(resultSet.getString("COLUMN_NAME"));
                column.setType(DataTypeMapping.valueOf(resultSet.getString("TYPE_NAME").replace(" ", "_")));
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

    /**
     * Execute all querySet DDLs first based on tenantId if specified. This is executed
     * first since we don't want to run DDLs in parallel to executing queries.
     *
     * @param querySet
     * @throws Exception
     */
    public void executeQuerySetDdls(QuerySet querySet) throws Exception {
        for (Query query : querySet.getQuery()) {
            if (null != query.getDdl()) {
                Connection conn = null;
                try {
                    LOGGER.info("\nExecuting DDL:" + query.getDdl() + " on tenantId:" + query
                            .getTenantId());
                    executeStatement(query.getDdl(),
                            conn = getConnection(query.getTenantId()));
                } finally {
                    if (null != conn) {
                        conn.close();
                    }
                }
            }
        }
    }
    
    /**
     * Executes any ddl defined at the scenario level. This is executed before we commence
     * the data load.
     * 
     * @throws Exception
     */
    public void executeScenarioDdl(List<Ddl> ddls, String tenantId, DataLoadTimeSummary dataLoadTimeSummary) throws Exception {
        if (null != ddls) {
            Connection conn = null;
            try {
            	for (Ddl ddl : ddls) {
                    LOGGER.info("\nExecuting DDL:" + ddl + " on tenantId:" +tenantId);
	                long startTime = EnvironmentEdgeManager.currentTimeMillis();
	                executeStatement(ddl.toString(), conn = getConnection(tenantId));
	                if (ddl.getStatement().toUpperCase().contains(ASYNC_KEYWORD)) {
	                	waitForAsyncIndexToFinish(ddl.getTableName());
	                }
	                dataLoadTimeSummary.add(ddl.getTableName(), 0,
                        (int)(EnvironmentEdgeManager.currentTimeMillis() - startTime));
            	}
            } finally {
                if (null != conn) {
                    conn.close();
                }
            }
        }
    }

    /**
     * Waits for ASYNC index to build
     * @param tableName
     * @throws InterruptedException
     */
    private void waitForAsyncIndexToFinish(String tableName) throws InterruptedException {
    	//Wait for up to 15 mins for ASYNC index build to start
    	boolean jobStarted = false;
    	for (int i=0; i<15; i++) {
    		if (isYarnJobInProgress(tableName)) {
    			jobStarted = true;
    			break;
    		}
    		Thread.sleep(ONE_MIN_IN_MS);
    	}
    	if (jobStarted == false) {
    		throw new IllegalStateException("ASYNC index build did not start within 15 mins");
    	}

    	// Wait till ASYNC index job finishes to get approximate job E2E time
    	for (;;) {
    		if (!isYarnJobInProgress(tableName))
    			break;
    		Thread.sleep(ONE_MIN_IN_MS);
    	}
    }
    
    /**
     * Checks if a YARN job with the specific table name is in progress
     * @param tableName
     * @return
     */
    boolean isYarnJobInProgress(String tableName) {
		try {
            LOGGER.info("Fetching YARN apps...");
			Set<String> response = new PhoenixMRJobSubmitter().getSubmittedYarnApps();
			for (String str : response) {
                LOGGER.info("Runnng YARN app: " + str);
				if (str.toUpperCase().contains(tableName.toUpperCase())) {
					return true;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
    }

	public static String getZookeeper() {
        return zookeeper;
    }

    public static void setZookeeper(String zookeeper) {
        LOGGER.info("Setting zookeeper: " + zookeeper);
        useThickDriver(zookeeper);
    }

    public static void useThickDriver(String zookeeper) {
        PhoenixUtil.useThinDriver = false;
        PhoenixUtil.zookeeper = Objects.requireNonNull(zookeeper);
    }

    public static int getRowCountOverride() {
        return rowCountOverride;
    }

    public static void setRowCountOverride(int rowCountOverride) {
        PhoenixUtil.rowCountOverride = rowCountOverride;
    }

    /**
     * Update Phoenix table stats
     *
     * @param tableName
     * @throws Exception
     */
    public void updatePhoenixStats(String tableName, Scenario scenario) throws Exception {
        LOGGER.info("Updating stats for " + tableName);
        executeStatement("UPDATE STATISTICS " + tableName, scenario);
    }

    public String getExplainPlan(Query query) throws SQLException {
    	return getExplainPlan(query, null, null);
    }
    
    /**
     * Get explain plan for a query
     *
     * @param query
     * @param ruleApplier 
     * @param scenario 
     * @return
     * @throws SQLException
     */
    public String getExplainPlan(Query query, Scenario scenario, RulesApplier ruleApplier) throws SQLException {
        Connection conn = null;
        ResultSet rs = null;
        PreparedStatement statement = null;
        StringBuilder buf = new StringBuilder();
        try {
            conn = getConnection(query.getTenantId());
            String explainQuery;
            if (scenario != null && ruleApplier != null) {
            	explainQuery = query.getDynamicStatement(ruleApplier, scenario);
            }
            else {
            	explainQuery = query.getStatement();
            }
            
            statement = conn.prepareStatement("EXPLAIN " + explainQuery);
            rs = statement.executeQuery();
            while (rs.next()) {
                buf.append(rs.getString(1).trim().replace(",", "-"));
            }
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) rs.close();
            if (statement != null) statement.close();
            if (conn != null) conn.close();
        }
        return buf.toString();
    }
}
