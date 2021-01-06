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

import org.apache.phoenix.thirdparty.com.google.common.base.Function;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.Ddl;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.mt.OperationStats;
import org.apache.phoenix.pherf.workload.mt.PreScenarioOperation;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * A supplier of {@link Function} that takes {@link PreScenarioOperation} as an input
 */
class PreScenarioOperationSupplier extends BaseOperationSupplier {
    private static final Logger LOGGER = LoggerFactory.getLogger(PreScenarioOperationSupplier.class);

    public PreScenarioOperationSupplier(PhoenixUtil phoenixUtil, DataModel model, Scenario scenario) {
        super(phoenixUtil, model, scenario);
    }

    @Override
    public Function<TenantOperationInfo, OperationStats> get() {
        return new Function<TenantOperationInfo, OperationStats>() {

            @Override
            public OperationStats apply(final TenantOperationInfo input) {
                final PreScenarioOperation operation = (PreScenarioOperation) input.getOperation();
                final String tenantId = input.getTenantId();
                final String tenantGroup = input.getTenantGroupId();
                final String opGroup = input.getOperationGroupId();
                final String tableName = input.getTableName();
                final String scenarioName = input.getScenarioName();
                final String opName = String.format("%s:%s:%s:%s:%s",
                        scenarioName, tableName, opGroup, tenantGroup, tenantId);

                long startTime = EnvironmentEdgeManager.currentTimeMillis();
                int status = 0;
                if (!operation.getPreScenarioDdls().isEmpty()) {
                    try (Connection conn = phoenixUtil.getConnection(tenantId)) {
                        for (Ddl ddl : operation.getPreScenarioDdls()) {
                            LOGGER.info("\nExecuting DDL:" + ddl + " on tenantId:" + tenantId);
                            phoenixUtil.executeStatement(ddl.toString(), conn);
                            if (ddl.getStatement().toUpperCase().contains(phoenixUtil.ASYNC_KEYWORD)) {
                                phoenixUtil.waitForAsyncIndexToFinish(ddl.getTableName());
                            }
                        }
                    } catch (SQLException sqle) {
                        LOGGER.error("Operation " + opName + " failed with exception ", sqle);
                        status = -1;
                    } catch (Exception e) {
                        LOGGER.error("Operation " + opName + " failed with exception ", e);
                        status = -1;
                    }
                }
                long totalDuration = EnvironmentEdgeManager.currentTimeMillis() - startTime;
                return new OperationStats(input, startTime, status, operation.getPreScenarioDdls().size(), totalDuration);
            }
        };
    }
}
