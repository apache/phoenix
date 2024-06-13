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

import com.google.gson.Gson;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.mapreduce.index.automation.PhoenixMRJobSubmitter;
import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.configuration.Column;
import org.apache.phoenix.pherf.configuration.DataTypeMapping;
import org.apache.phoenix.pherf.configuration.Ddl;
import org.apache.phoenix.pherf.configuration.Query;
import org.apache.phoenix.pherf.configuration.QuerySet;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.result.DataLoadTimeSummary;
import org.apache.phoenix.pherf.rules.DataValue;
import org.apache.phoenix.pherf.rules.RulesApplier;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;

public class PhoenixUtil {
    public static final String ASYNC_KEYWORD = "ASYNC";
    public static final Gson GSON = new Gson();
    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixUtil.class);
    private static String zookeeper;
    private static int rowCountOverride = 0;
    private boolean testEnabled;
    private static PhoenixUtil instance;
    private static boolean useThinDriver;
    private static String queryServerUrl;
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

    public static Gson getGSON() {
        return GSON;
    }

    public Connection getConnection() throws Exception {
        return getConnection(null);
    }

    public Connection getConnection(String tenantId) throws Exception {
        return getConnection(tenantId, testEnabled, null);
    }

    public Connection getConnection(String tenantId,
                                    Properties properties) throws  Exception {
        Map<String, String> propertyHashMap = getPropertyHashMap(properties);
        return getConnection(tenantId, testEnabled, propertyHashMap);
    }
    
    public Connection getConnection(String tenantId,
                                    Map<String, String> propertyHashMap) throws Exception {
        return getConnection(tenantId, testEnabled, propertyHashMap);
    }

    public Connection getConnection(String tenantId, boolean testEnabled,
                                    Map<String, String> propertyHashMap) throws Exception {
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
            
            if (propertyHashMap != null) {
            	for (Map.Entry<String, String> phxProperty: propertyHashMap.entrySet()) {
            		props.setProperty(phxProperty.getKey(), phxProperty.getValue());
					LOGGER.debug("Setting connection property "
                            + phxProperty.getKey() + " to "
                            + phxProperty.getValue());
            	}
            }
            
            String url = "jdbc:phoenix:" + zookeeper + (testEnabled ? ";test=true" : "");
            return DriverManager.getConnection(url, props);
        }
    }

    private Map<String, String> getPropertyHashMap(Properties props) {
        Map<String, String> propsMaps = new HashMap<>();
        for (String prop : props.stringPropertyNames()) {
            propsMaps.put(prop, props.getProperty(prop));
        }
        return propsMaps;
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

    public void dropChildView(RegionCoprocessorEnvironment taskRegionEnvironment, int depth) {
        TaskRegionObserver.SelfHealingTask task =
                new TaskRegionObserver.SelfHealingTask(
                        taskRegionEnvironment, QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS);
        for (int i = 0; i < depth; i++) {
            task.run();
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
                LOGGER.debug(String.format("getColumnsMetaData for column name : %s", column.getName()));
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
    public void waitForAsyncIndexToFinish(String tableName) throws InterruptedException {
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

    public PreparedStatement buildStatement(RulesApplier rulesApplier, Scenario scenario, List<Column> columns,
            PreparedStatement statement, SimpleDateFormat simpleDateFormat) throws Exception {

        int count = 1;
        for (Column column : columns) {
            DataValue dataValue = rulesApplier.getDataForRule(scenario, column);
            switch (column.getType()) {
            case VARCHAR:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.VARCHAR);
                } else {
                    statement.setString(count, dataValue.getValue());
                }
                break;
            case CHAR:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.CHAR);
                } else {
                    statement.setString(count, dataValue.getValue());
                }
                break;
            case DECIMAL:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.DECIMAL);
                } else {
                    statement.setBigDecimal(count, new BigDecimal(dataValue.getValue()));
                }
                break;
            case INTEGER:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.INTEGER);
                } else {
                    statement.setInt(count, Integer.parseInt(dataValue.getValue()));
                }
                break;
            case UNSIGNED_LONG:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.OTHER);
                } else {
                    statement.setLong(count, Long.parseLong(dataValue.getValue()));
                }
                break;
            case BIGINT:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.BIGINT);
                } else {
                    statement.setLong(count, Long.parseLong(dataValue.getValue()));
                }
                break;
            case TINYINT:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.TINYINT);
                } else {
                    statement.setLong(count, Integer.parseInt(dataValue.getValue()));
                }
                break;
            case DATE:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.DATE);
                } else {
                    Date
                            date =
                            new java.sql.Date(simpleDateFormat.parse(dataValue.getValue()).getTime());
                    statement.setDate(count, date);
                }
                break;
            case VARCHAR_ARRAY:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.ARRAY);
                } else {
                    Array
                            arr =
                            statement.getConnection().createArrayOf("VARCHAR", dataValue.getValue().split(","));
                    statement.setArray(count, arr);
                }
                break;
            case VARBINARY:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.VARBINARY);
                } else {
                    statement.setBytes(count, dataValue.getValue().getBytes());
                }
                break;
            case TIMESTAMP:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.TIMESTAMP);
                } else {
                    java.sql.Timestamp
                            ts =
                            new java.sql.Timestamp(simpleDateFormat.parse(dataValue.getValue()).getTime());
                    statement.setTimestamp(count, ts);
                }
                break;
            default:
                break;
            }
            count++;
        }
        return statement;
    }

    public String buildSql(final List<Column> columns, final String tableName) {
        StringBuilder builder = new StringBuilder();
        builder.append("upsert into ");
        builder.append(tableName);
        builder.append(" (");
        int count = 1;
        for (Column column : columns) {
            builder.append(column.getName());
            if (count < columns.size()) {
                builder.append(",");
            } else {
                builder.append(")");
            }
            count++;
        }
        builder.append(" VALUES (");
        for (int i = 0; i < columns.size(); i++) {
            if (i < columns.size() - 1) {
                builder.append("?,");
            } else {
                builder.append("?)");
            }
        }
        return builder.toString();
    }

    public org.apache.hadoop.hbase.util.Pair<Long, Long> getResults(
            Query query,
            ResultSet rs,
            String queryIteration,
            boolean isSelectCountStatement,
            Long queryStartTime) throws Exception {

        Long resultRowCount = 0L;
        while (rs.next()) {
            if (isSelectCountStatement) {
                resultRowCount = rs.getLong(1);
            } else {
                resultRowCount++;
            }
            long queryElapsedTime = EnvironmentEdgeManager.currentTimeMillis() - queryStartTime;
            if (queryElapsedTime >= query.getTimeoutDuration()) {
                LOGGER.error("Query " + queryIteration + " exceeded timeout of "
                        +  query.getTimeoutDuration() + " ms at " + queryElapsedTime + " ms.");
                return new org.apache.hadoop.hbase.util.Pair(resultRowCount, queryElapsedTime);
            }
        }
        return new org.apache.hadoop.hbase.util.Pair(resultRowCount, EnvironmentEdgeManager.currentTimeMillis() - queryStartTime);
    }

}
