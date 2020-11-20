/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.pherf.workload.mt.tenantoperation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import org.apache.phoenix.pherf.configuration.Column;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.Ddl;
import org.apache.phoenix.pherf.configuration.LoadProfile;
import org.apache.phoenix.pherf.configuration.Noop;
import org.apache.phoenix.pherf.configuration.Query;
import org.apache.phoenix.pherf.configuration.QuerySet;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.configuration.TenantGroup;
import org.apache.phoenix.pherf.configuration.Upsert;
import org.apache.phoenix.pherf.configuration.UserDefined;
import org.apache.phoenix.pherf.configuration.XMLConfigParser;
import org.apache.phoenix.pherf.rules.DataValue;
import org.apache.phoenix.pherf.rules.RulesApplier;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.mt.EventGenerator;
import org.apache.phoenix.pherf.workload.mt.NoopOperation;
import org.apache.phoenix.pherf.workload.mt.Operation;
import org.apache.phoenix.pherf.workload.mt.OperationStats;
import org.apache.phoenix.pherf.workload.mt.PreScenarioOperation;
import org.apache.phoenix.pherf.workload.mt.QueryOperation;
import org.apache.phoenix.pherf.workload.mt.UpsertOperation;
import org.apache.phoenix.pherf.workload.mt.UserDefinedOperation;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Factory class for operations.
 * The class is responsible for creating new instances of various operation types.
 * Operations typically implement @see {@link TenantOperationImpl}
 * Operations that need to be executed are generated
 * by @see {@link EventGenerator}
 */
public class TenantOperationFactory {

    private static class TenantView {
        private final String tenantId;
        private final String viewName;

        public TenantView(String tenantId, String viewName) {
            this.tenantId = tenantId;
            this.viewName = viewName;
        }

        public String getTenantId() {
            return tenantId;
        }

        public String getViewName() {
            return viewName;
        }
    }
    private static final Logger LOGGER = LoggerFactory.getLogger(TenantOperationFactory.class);
    private final PhoenixUtil phoenixUtil;
    private final DataModel model;
    private final Scenario scenario;
    private final XMLConfigParser parser;

    private final RulesApplier rulesApplier;
    private final LoadProfile loadProfile;
    private final List<Operation> operationList = Lists.newArrayList();

    private final BloomFilter<TenantView> tenantsLoaded;

    public TenantOperationFactory(PhoenixUtil phoenixUtil, DataModel model, Scenario scenario) {
        this.phoenixUtil = phoenixUtil;
        this.model = model;
        this.scenario = scenario;
        this.parser = null;
        this.rulesApplier = new RulesApplier(model);
        this.loadProfile = this.scenario.getLoadProfile();
        Funnel<TenantView> tenantViewFunnel = new Funnel<TenantView>() {
            @Override
            public void funnel(TenantView tenantView, PrimitiveSink into) {
                into.putString(tenantView.getTenantId(), Charsets.UTF_8)
                        .putString(tenantView.getViewName(), Charsets.UTF_8);
            }
        };

        int numTenants = 0;
        for (TenantGroup tg : loadProfile.getTenantDistribution()) {
            numTenants += tg.getNumTenants();
        }

        // This holds the info whether the tenant view was created (initialized) or not.
        tenantsLoaded = BloomFilter.create(tenantViewFunnel, numTenants, 0.01);

        // Read the scenario definition and load the various operations.
        for (final Noop noOp : scenario.getNoop()) {
            Operation noopOperation = new NoopOperation() {
                @Override public Noop getNoop() {
                    return noOp;
                }
                @Override public String getId() {
                    return noOp.getId();
                }

                @Override public OperationType getType() {
                    return OperationType.NO_OP;
                }
            };
            operationList.add(noopOperation);
        }

        for (final Upsert upsert : scenario.getUpsert()) {
            Operation upsertOp = new UpsertOperation() {
                @Override public Upsert getUpsert() {
                    return upsert;
                }

                @Override public String getId() {
                    return upsert.getId();
                }

                @Override public OperationType getType() {
                    return OperationType.UPSERT;
                }
            };
            operationList.add(upsertOp);
        }
        for (final QuerySet querySet : scenario.getQuerySet()) {
            for (final Query query : querySet.getQuery()) {
                Operation queryOp = new QueryOperation() {
                    @Override public Query getQuery() {
                        return query;
                    }

                    @Override public String getId() {
                        return query.getId();
                    }

                    @Override public OperationType getType() {
                        return OperationType.SELECT;
                    }
                };
                operationList.add(queryOp);
            }
        }

        for (final UserDefined udf : scenario.getUdf()) {
            Operation udfOperation = new UserDefinedOperation() {
                @Override public UserDefined getUserFunction() {
                    return udf;
                }

                @Override public String getId() {
                    return udf.getId();
                }

                @Override public OperationType getType() {
                    return OperationType.USER_DEFINED;
                }
            };
            operationList.add(udfOperation);
        }
    }

    public PhoenixUtil getPhoenixUtil() {
        return phoenixUtil;
    }

    public DataModel getModel() {
        return model;
    }

    public Scenario getScenario() {
        return scenario;
    }

    public List<Operation> getOperationsForScenario() {
        return operationList;
    }

    public TenantOperationImpl getOperation(final TenantOperationInfo input) {
        TenantView tenantView = new TenantView(input.getTenantId(), scenario.getTableName());

        // Check if pre run ddls are needed.
        if (!tenantsLoaded.mightContain(tenantView)) {
            // Initialize the tenant using the pre scenario ddls.
            final PreScenarioOperation operation = new PreScenarioOperation() {
                @Override public List<Ddl> getPreScenarioDdls() {
                    List<Ddl> ddls = scenario.getPreScenarioDdls();
                    return ddls == null ? Lists.<Ddl>newArrayList() : ddls;
                }

                @Override public String getId() {
                    return OperationType.PRE_RUN.name();
                }

                @Override public OperationType getType() {
                    return OperationType.PRE_RUN;
                }
            };
            // Initialize with the pre run operation.
            TenantOperationInfo preRunSample = new TenantOperationInfo(
                    input.getModelName(),
                    input.getScenarioName(),
                    input.getTableName(),
                    input.getTenantGroupId(),
                    Operation.OperationType.PRE_RUN.name(),
                    input.getTenantId(), operation);

            TenantOperationImpl impl = new PreScenarioTenantOperationImpl();
            try {
                // Run the initialization operation.
                OperationStats stats = impl.getMethod().apply(preRunSample);
                LOGGER.info(phoenixUtil.getGSON().toJson(stats));
            } catch (Exception e) {
                LOGGER.error(
                        String.format("Failed to initialize tenant. [%s, %s] ",
                                tenantView.tenantId,
                                tenantView.viewName
                        ), e.fillInStackTrace());
            }
            tenantsLoaded.put(tenantView);
        }

        switch (input.getOperation().getType()) {
        case NO_OP:
            return new NoopTenantOperationImpl();
        case SELECT:
            return new QueryTenantOperationImpl();
        case UPSERT:
            return new UpsertTenantOperationImpl();
        case USER_DEFINED:
            return new UserDefinedOperationImpl();
        default:
            throw new IllegalArgumentException("Unknown operation type");
        }
    }

    class QueryTenantOperationImpl implements TenantOperationImpl {

        @Override public Function<TenantOperationInfo, OperationStats> getMethod() {
            return new Function<TenantOperationInfo, OperationStats>() {

                @Nullable @Override public OperationStats apply(@Nullable TenantOperationInfo input) {
                    final QueryOperation operation = (QueryOperation) input.getOperation();
                    final String tenantGroup = input.getTenantGroupId();
                    final String opGroup = input.getOperationGroupId();
                    final String tenantId = input.getTenantId();
                    final String scenarioName = input.getScenarioName();
                    final String tableName = input.getTableName();
                    final Query query = operation.getQuery();
                    final long opCounter = 1;

                    String opName = String.format("%s:%s:%s:%s:%s", scenarioName, tableName,
                            opGroup, tenantGroup, tenantId);
                    LOGGER.info("\nExecuting query " + query.getStatement());
                    // TODO add explain plan output to the stats.

                    Connection conn = null;
                    PreparedStatement statement = null;
                    ResultSet rs = null;
                    Long startTime = EnvironmentEdgeManager.currentTimeMillis();
                    Long resultRowCount = 0L;
                    Long queryElapsedTime = 0L;
                    String queryIteration = opName + ":" + opCounter;
                    try {
                        conn = phoenixUtil.getConnection(tenantId);
                        conn.setAutoCommit(true);
                        // TODO dynamic statements
                        //final String statementString = query.getDynamicStatement(rulesApplier, scenario);
                        statement = conn.prepareStatement(query.getStatement());
                        boolean isQuery = statement.execute();
                        if (isQuery) {
                            rs = statement.getResultSet();
                            boolean isSelectCountStatement = query.getStatement().toUpperCase().trim().contains("COUNT(") ? true : false;
                            org.apache.hadoop.hbase.util.Pair<Long, Long>
                                    r = phoenixUtil.getResults(query, rs, queryIteration, isSelectCountStatement, startTime);
                            resultRowCount = r.getFirst();
                            queryElapsedTime = r.getSecond();
                        } else {
                            conn.commit();
                        }
                    } catch (Exception e) {
                        LOGGER.error("Exception while executing query iteration " + queryIteration, e);
                    } finally {
                        try {
                            if (rs != null) rs.close();
                            if (statement != null) statement.close();
                            if (conn != null) conn.close();

                        } catch (Throwable t) {
                            // swallow;
                        }
                    }
                    return new OperationStats(input, startTime, 0, resultRowCount, queryElapsedTime);
                }
            };
        }
    }

    class UpsertTenantOperationImpl implements TenantOperationImpl {

        @Override public Function<TenantOperationInfo, OperationStats> getMethod() {
            return new Function<TenantOperationInfo, OperationStats>() {

                @Nullable @Override public OperationStats apply(@Nullable TenantOperationInfo input) {

                    final int batchSize = loadProfile.getBatchSize();
                    final boolean useBatchApi = batchSize != 0;
                    final int rowCount = useBatchApi ? batchSize : 1;

                    final UpsertOperation operation = (UpsertOperation) input.getOperation();
                    final String tenantGroup = input.getTenantGroupId();
                    final String opGroup = input.getOperationGroupId();
                    final String tenantId = input.getTenantId();
                    final Upsert upsert = operation.getUpsert();
                    final String tableName = input.getTableName();
                    final String scenarioName = input.getScenarioName();
                    final List<Column> columns = upsert.getColumn();

                    final String opName = String.format("%s:%s:%s:%s:%s",
                            scenarioName, tableName, opGroup, tenantGroup, tenantId);

                    long rowsCreated = 0;
                    long startTime = 0, duration, totalDuration;
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    try (Connection connection = phoenixUtil.getConnection(tenantId)) {
                        connection.setAutoCommit(true);
                        startTime = EnvironmentEdgeManager.currentTimeMillis();
                        String sql = phoenixUtil.buildSql(columns, tableName);
                        PreparedStatement stmt = null;
                        try {
                            stmt = connection.prepareStatement(sql);
                            for (long i = rowCount; i > 0; i--) {
                                LOGGER.debug("Operation " + opName + " executing ");
                                stmt = phoenixUtil.buildStatement(rulesApplier, scenario, columns, stmt, simpleDateFormat);
                                if (useBatchApi) {
                                    stmt.addBatch();
                                } else {
                                    rowsCreated += stmt.executeUpdate();
                                }
                            }
                        } catch (SQLException e) {
                            LOGGER.error("Operation " + opName + " failed with exception ", e);
                            throw e;
                        } finally {
                            // Need to keep the statement open to send the remaining batch of updates
                            if (!useBatchApi && stmt != null) {
                                stmt.close();
                            }
                            if (connection != null) {
                                if (useBatchApi && stmt != null) {
                                    int[] results = stmt.executeBatch();
                                    for (int x = 0; x < results.length; x++) {
                                        int result = results[x];
                                        if (result < 1) {
                                            final String msg =
                                                    "Failed to write update in batch (update count="
                                                            + result + ")";
                                            throw new RuntimeException(msg);
                                        }
                                        rowsCreated += result;
                                    }
                                    // Close the statement after our last batch execution.
                                    stmt.close();
                                }

                                try {
                                    connection.commit();
                                    duration = EnvironmentEdgeManager.currentTimeMillis() - startTime;
                                    LOGGER.info("Writer ( " + Thread.currentThread().getName()
                                            + ") committed Final Batch. Duration (" + duration + ") Ms");
                                    connection.close();
                                } catch (SQLException e) {
                                    // Swallow since we are closing anyway
                                    e.printStackTrace();
                                }
                            }
                        }
                    } catch (SQLException throwables) {
                        throw new RuntimeException(throwables);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                    totalDuration = EnvironmentEdgeManager.currentTimeMillis() - startTime;
                    return new OperationStats(input, startTime, 0, rowsCreated, totalDuration);
                }
            };
        }
    }

    class PreScenarioTenantOperationImpl implements TenantOperationImpl {

        @Override public Function<TenantOperationInfo, OperationStats> getMethod() {
            return new Function<TenantOperationInfo, OperationStats>() {
                @Override public OperationStats apply(final TenantOperationInfo input) {
                    final PreScenarioOperation operation = (PreScenarioOperation) input.getOperation();
                    final String tenantId = input.getTenantId();
                    final String tableName = scenario.getTableName();

                    long startTime = EnvironmentEdgeManager.currentTimeMillis();
                    if (!operation.getPreScenarioDdls().isEmpty()) {
                        try (Connection conn = phoenixUtil.getConnection(tenantId)) {
                            for (Ddl ddl : scenario.getPreScenarioDdls()) {
                                LOGGER.info("\nExecuting DDL:" + ddl + " on tenantId:" + tenantId);
                                phoenixUtil.executeStatement(ddl.toString(), conn);
                                if (ddl.getStatement().toUpperCase().contains(phoenixUtil.ASYNC_KEYWORD)) {
                                    phoenixUtil.waitForAsyncIndexToFinish(ddl.getTableName());
                                }
                            }
                        } catch (SQLException throwables) {
                            throw new RuntimeException(throwables);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                    long totalDuration = EnvironmentEdgeManager.currentTimeMillis() - startTime;
                    return new OperationStats(input, startTime,0, operation.getPreScenarioDdls().size(), totalDuration);

                }
            };
        }
    }

    @VisibleForTesting
    class NoopTenantOperationImpl implements TenantOperationImpl {

        @Override public Function<TenantOperationInfo, OperationStats> getMethod() {
            return new Function<TenantOperationInfo, OperationStats>() {
                @Override public OperationStats apply(final TenantOperationInfo input) {

                    final NoopOperation operation = (NoopOperation) input.getOperation();
                    final Noop noop = operation.getNoop();

                    long startTime = EnvironmentEdgeManager.currentTimeMillis();
                    // Sleep for the specified time to simulate idle time.
                    try {
                        TimeUnit.MILLISECONDS.sleep(noop.getIdleTime());
                        long duration = EnvironmentEdgeManager.currentTimeMillis() - startTime;
                        return new OperationStats(input, startTime, 0, 0, duration);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        long duration = EnvironmentEdgeManager.currentTimeMillis() - startTime;
                        return new OperationStats(input, startTime,-1, 0, duration);
                    }
                }
            };
        }
    }

    class UserDefinedOperationImpl implements TenantOperationImpl {

        @Override public Function<TenantOperationInfo, OperationStats> getMethod() {
            return new Function<TenantOperationInfo, OperationStats>() {
                @Override public OperationStats apply(final TenantOperationInfo input) {
                    // TODO : implement user defined operation invocation.
                    long startTime = EnvironmentEdgeManager.currentTimeMillis();
                    long duration = EnvironmentEdgeManager.currentTimeMillis() - startTime;
                    return new OperationStats(input, startTime,0, 0, duration);
                }
            };
        }
    }


}
