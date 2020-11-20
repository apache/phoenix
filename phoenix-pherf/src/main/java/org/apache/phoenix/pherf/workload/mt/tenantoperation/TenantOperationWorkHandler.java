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

import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.WorkHandler;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.workload.mt.OperationStats;
import org.apache.phoenix.pherf.workload.mt.tenantoperation.TenantOperationWorkload.TenantOperationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO Documentation
 */

public class TenantOperationWorkHandler implements WorkHandler<TenantOperationEvent>,
        LifecycleAware {
    private static final Logger LOGGER = LoggerFactory.getLogger(TenantOperationWorkHandler.class);
    private final String handlerId;
    private final TenantOperationFactory operationFactory;


    public TenantOperationWorkHandler(TenantOperationFactory operationFactory,
            String handlerId) {
        this.handlerId = handlerId;
        this.operationFactory = operationFactory;
    }

    @Override public void onEvent(TenantOperationEvent event)
            throws Exception {
        TenantOperationInfo input = event.getTenantOperationInfo();
        TenantOperationImpl op = operationFactory.getOperation(input);
        OperationStats stats = op.getMethod().apply(input);
        stats.setHandlerId(handlerId);
        LOGGER.info(operationFactory.getPhoenixUtil().getGSON().toJson(stats));
    }

    @Override public void onStart() {
        Scenario scenario = operationFactory.getScenario();
        LOGGER.info(String.format("TenantOperationWorkHandler started for %s:%s",
                scenario.getName(), scenario.getTableName()));
    }

    @Override public void onShutdown() {
        Scenario scenario = operationFactory.getScenario();
        LOGGER.info(String.format("TenantOperationWorkHandler stopped for %s:%s",
                scenario.getName(), scenario.getTableName()));
    }
}
