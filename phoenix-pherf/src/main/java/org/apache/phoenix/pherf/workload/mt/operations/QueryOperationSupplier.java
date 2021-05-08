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

package org.apache.phoenix.pherf.workload.mt.operations;

import org.apache.phoenix.pherf.configuration.TenantGroup;
import org.apache.phoenix.pherf.workload.mt.generators.TenantOperationInfo;
import org.apache.phoenix.thirdparty.com.google.common.base.Function;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.Query;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * A supplier of {@link Function} that takes {@link QueryOperation} as an input.
 */
public class QueryOperationSupplier extends BaseOperationSupplier {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryOperationSupplier.class);

    public QueryOperationSupplier(PhoenixUtil phoenixUtil, DataModel model, Scenario scenario) {
        super(phoenixUtil, model, scenario);
    }

    @Override
    public Function<TenantOperationInfo, OperationStats> get() {
        return new Function<TenantOperationInfo, OperationStats>() {

            @Override
            public OperationStats apply(final TenantOperationInfo input) {
                Preconditions.checkNotNull(input);
                final QueryOperation operation = (QueryOperation) input.getOperation();
                final Query query = operation.getQuery();
                final String tenantGroup = input.getTenantGroupId();
                final String opGroup = input.getOperationGroupId();
                final String scenarioName = input.getScenarioName();
                final String tableName = input.getTableName();

                // TODO:
                // Ideally the fact that the op needs to executed using global connection
                // needs to be built into the framework and injected during event generation.
                // For now a special tenant whose id = "TGLOBAL00000001" will be logged.
                final boolean isTenantGroupGlobal = (tenantGroup.compareTo(TenantGroup.DEFAULT_GLOBAL_ID) == 0);
                final String tenantId = isTenantGroupGlobal || query.isUseGlobalConnection() ? null : input.getTenantId();

                String opName = String.format("%s:%s:%s:%s:%s", scenarioName, tableName,
                        opGroup, tenantGroup, input.getTenantId());
                LOGGER.debug("\nExecuting query " + query.getStatement());

                long startTime = 0;
                int status = 0;
                Long resultRowCount = 0L;
                Long queryElapsedTime = 0L;
                try (Connection connection = phoenixUtil.getConnection(tenantId)) {
                    startTime = EnvironmentEdgeManager.currentTimeMillis();

                    // TODO handle dynamic statements
                    try (PreparedStatement statement = connection.prepareStatement(query.getStatement())) {
                        try (ResultSet rs = statement.executeQuery()) {
                            boolean isSelectCountStatement = query.getStatement().toUpperCase().trim().contains("COUNT(") ? true : false;
                            Pair<Long, Long> r = phoenixUtil.getResults(query, rs, opName,
                                    isSelectCountStatement, startTime);
                            resultRowCount = r.getFirst();
                            queryElapsedTime = r.getSecond();
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("Operation " + opName + " failed with exception ", e);
                    status = -1;
                }
                return new OperationStats(input, startTime, status, resultRowCount, queryElapsedTime);
            }
        };
    }
}
