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

package org.apache.phoenix.pherf.workload.mt.handlers;

import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.WorkHandler;
import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.result.ResultValue;
import org.apache.phoenix.pherf.workload.mt.MultiTenantWorkload;
import org.apache.phoenix.pherf.workload.mt.generators.BaseLoadEventGenerator.TenantOperationEvent;
import org.apache.phoenix.pherf.workload.mt.generators.TenantOperationInfo;
import org.apache.phoenix.pherf.workload.mt.operations.Operation;
import org.apache.phoenix.pherf.workload.mt.operations.OperationStats;
import org.apache.phoenix.pherf.workload.mt.operations.TenantOperationFactory;
import org.apache.phoenix.thirdparty.com.google.common.base.Function;
import org.apache.phoenix.thirdparty.com.google.common.base.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;

/**
 * A handler {@link WorkHandler} for simple orchestrations using the supplied rendezvous points
 * The handler will wait for the rendezvous to happen before executing the operations {@link Operation}
 * The handlers as in the basic {@link TenantOperationWorkHandler} will pick up the operation events
 * as and when they become available on the {@link com.lmax.disruptor.RingBuffer}
 * when published by the workload generator {@link MultiTenantWorkload}
 */

public class RendezvousingWorkHandler implements PherfWorkHandler<TenantOperationEvent>,
        LifecycleAware {
    private static final Logger LOGGER = LoggerFactory.getLogger(RendezvousingWorkHandler.class);
    private final String handlerId;
    private final TenantOperationFactory operationFactory;
    private final Map<String, CyclicBarrier> rendezvousPoints;

    public RendezvousingWorkHandler(TenantOperationFactory operationFactory,
            String handlerId, Map<String, CyclicBarrier> rendezvousPoints) {
        this.handlerId = handlerId;
        this.operationFactory = operationFactory;
        this.rendezvousPoints = rendezvousPoints;
    }

    @Override
    public void onEvent(TenantOperationEvent event)
            throws Exception {
        TenantOperationInfo input = event.getTenantOperationInfo();
        Supplier<Function<TenantOperationInfo, OperationStats>> opSupplier =
                operationFactory.getOperationSupplier(input);

        boolean startRendezvousEnabled = rendezvousPoints.containsKey(PherfConstants.MT_HANDLER_START_RENDEZVOUS_PROP_KEY);
        if (startRendezvousEnabled) {
            rendezvousPoints.get(PherfConstants.MT_HANDLER_START_RENDEZVOUS_PROP_KEY).await();
        }
        OperationStats stats = opSupplier.get().apply(input);
        stats.setHandlerId(handlerId);

        // TODO need to handle asynchronous result publishing
        boolean resultsRendezvousEnabled = rendezvousPoints.containsKey(PherfConstants.MT_HANDLER_RESULTS_RENDEZVOUS_PROP_KEY);
        if (resultsRendezvousEnabled) {
            rendezvousPoints.get(PherfConstants.MT_HANDLER_RESULTS_RENDEZVOUS_PROP_KEY).await();
        }
        LOGGER.info(operationFactory.getPhoenixUtil().getGSON().toJson(stats));
    }

    @Override
    public void onStart() {
        Scenario scenario = operationFactory.getScenario();
        LOGGER.info(String.format("TenantOperationWorkHandler started for %s:%s",
                scenario.getName(), scenario.getTableName()));
    }

    @Override
    public void onShutdown() {
        Scenario scenario = operationFactory.getScenario();
        LOGGER.info(String.format("TenantOperationWorkHandler stopped for %s:%s",
                scenario.getName(), scenario.getTableName()));
    }

    @Override public List<ResultValue<OperationStats>> getResults() {
        return new ArrayList<>();
    }
}
