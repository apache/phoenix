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

package org.apache.phoenix.pherf.workload.mt;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.Workload;
import org.apache.phoenix.pherf.workload.mt.generators.LoadEventGenerator;
import org.apache.phoenix.pherf.workload.mt.generators.TenantOperationInfo;
import org.apache.phoenix.pherf.workload.mt.handlers.PherfWorkHandler;
import org.apache.phoenix.pherf.workload.mt.generators.TenantLoadEventGeneratorFactory;
import org.apache.phoenix.pherf.workload.mt.handlers.TenantOperationWorkHandler;
import org.apache.phoenix.pherf.workload.mt.operations.TenantOperationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * This class creates workload for tenant based load profiles.
 * It uses @see {@link TenantOperationFactory} in conjunction with
 * @see {@link LoadEventGenerator} to generate the load events.
 * It then publishes these events onto a RingBuffer based queue.
 * The @see {@link TenantOperationWorkHandler} drains the events from the queue and executes them.
 * Reference for RingBuffer based queue http://lmax-exchange.github.io/disruptor/
 */

public class MultiTenantWorkload implements Workload {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiTenantWorkload.class);
    private final TenantLoadEventGeneratorFactory evtGeneratorFactory
            = new TenantLoadEventGeneratorFactory();
    private final LoadEventGenerator<TenantOperationInfo> generator;


    public MultiTenantWorkload(PhoenixUtil phoenixUtil, DataModel model, Scenario scenario,
            Properties properties) {
        this.generator =  evtGeneratorFactory.newLoadEventGenerator(phoenixUtil,
                model, scenario, properties);
    }

    public MultiTenantWorkload(PhoenixUtil phoenixUtil, DataModel model, Scenario scenario,
            List<PherfWorkHandler> workHandlers, Properties properties) throws Exception {
        this.generator =  evtGeneratorFactory.newLoadEventGenerator(phoenixUtil,
                model, scenario, workHandlers, properties);
    }

    @Override public Callable<Void> execute() throws Exception {
        return new Callable<Void>() {
            @Override public Void call() throws Exception {
                generator.start();
                return null;
            }
        };
    }

    @Override public void complete() {
        try {
            generator.stop();
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
    }
}
