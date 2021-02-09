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

package org.apache.phoenix.pherf.workload.mt.generators;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.mt.handlers.PherfWorkHandler;
import org.apache.phoenix.pherf.workload.mt.operations.TenantOperationFactory;
import org.apache.phoenix.pherf.workload.mt.handlers.TenantOperationWorkHandler;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;

/**
 * A base class for all load event generators.
 */
public abstract class BaseLoadEventGenerator
        implements LoadEventGenerator<TenantOperationInfo> {
    protected static final int DEFAULT_NUM_HANDLER_PER_SCENARIO = 4;
    protected static final int DEFAULT_BUFFER_SIZE = 8192;
    protected static final Logger LOGGER = LoggerFactory.getLogger(
            BaseLoadEventGenerator.class);

    protected Disruptor<TenantOperationEvent> disruptor;
    protected List<PherfWorkHandler> handlers;
    protected final Properties properties;

    protected final TenantOperationFactory operationFactory;
    protected final ExceptionHandler exceptionHandler;


    private static class WorkloadExceptionHandler implements ExceptionHandler {
        private static final Logger LOGGER = LoggerFactory.getLogger(WorkloadExceptionHandler.class);

        @Override public void handleEventException(Throwable ex, long sequence, Object event) {
            LOGGER.error("Sequence=" + sequence + ", event=" + event, ex);
            throw new RuntimeException(ex);
        }

        @Override public void handleOnStartException(Throwable ex) {
            LOGGER.error("On Start", ex);
            throw new RuntimeException(ex);
        }

        @Override public void handleOnShutdownException(Throwable ex) {
            LOGGER.error("On Shutdown", ex);
            throw new RuntimeException(ex);
        }
    }

    public static class TenantOperationEvent {
        TenantOperationInfo tenantOperationInfo;

        public TenantOperationInfo getTenantOperationInfo() {
            return tenantOperationInfo;
        }

        public void setTenantOperationInfo(TenantOperationInfo tenantOperationInfo) {
            this.tenantOperationInfo = tenantOperationInfo;
        }

        public static final EventFactory<TenantOperationEvent> EVENT_FACTORY = new EventFactory<TenantOperationEvent>() {
            public TenantOperationEvent newInstance() {
                return new TenantOperationEvent();
            }
        };
    }

    public BaseLoadEventGenerator(PhoenixUtil phoenixUtil, DataModel model, Scenario scenario,
            List<PherfWorkHandler> workers, Properties properties) {
        this(phoenixUtil, model, scenario, workers, new WorkloadExceptionHandler(), properties);
    }

    public BaseLoadEventGenerator(PhoenixUtil phoenixUtil, DataModel model,
            Scenario scenario, Properties properties) {
        this(phoenixUtil, model, scenario, null, new WorkloadExceptionHandler(), properties);
    }

    public BaseLoadEventGenerator(PhoenixUtil phoenixUtil, DataModel model,
            Scenario scenario,
            List<PherfWorkHandler> workers,
            ExceptionHandler exceptionHandler,
            Properties properties) {


        operationFactory = new TenantOperationFactory(phoenixUtil, model, scenario);
        if (scenario.getPhoenixProperties() != null) {
            properties.putAll(scenario.getPhoenixProperties());
        }
        this.properties = properties;

        if (workers == null || workers.isEmpty()) {
            workers = getWorkHandlers(properties);
        }
        this.handlers = workers;
        this.exceptionHandler = exceptionHandler;
    }


    @Override public PhoenixUtil getPhoenixUtil() { return operationFactory.getPhoenixUtil(); }

    @Override public Scenario getScenario() {
        return operationFactory.getScenario();
    }

    @Override public DataModel getModel() {
        return operationFactory.getModel();
    }

    @Override public Properties getProperties() {
        return this.properties;
    }

    @Override public TenantOperationFactory getOperationFactory() {
        return operationFactory;
    }

    @Override public void start() throws Exception {
        Scenario scenario = operationFactory.getScenario();
        String currentThreadName = Thread.currentThread().getName();
        int bufferSize = DEFAULT_BUFFER_SIZE;
        if (properties.containsKey("pherf.mt.buffer_size_per_scenario")) {
            bufferSize = Integer.parseInt((String)properties.get("pherf.mt.buffer_size_per_scenario"));
        }

        disruptor = new Disruptor<>(TenantOperationEvent.EVENT_FACTORY, bufferSize,
                new ThreadFactoryBuilder()
                        .setNameFormat(currentThreadName + "." + scenario.getName())
                        .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER)
                        .build(),
                ProducerType.SINGLE, new BlockingWaitStrategy());

        this.disruptor.setDefaultExceptionHandler(this.exceptionHandler);
        this.disruptor.handleEventsWithWorkerPool(this.handlers.toArray(new WorkHandler[] {}));
        RingBuffer<TenantOperationEvent> ringBuffer = this.disruptor.start();
        long numOperations = scenario.getLoadProfile().getNumOperations();
        while (numOperations > 0) {
            TenantOperationInfo sample = next();
            operationFactory.initializeTenant(sample);
            --numOperations;
            // Publishers claim events in sequence
            long sequence = ringBuffer.next();
            TenantOperationEvent event = ringBuffer.get(sequence);
            event.setTenantOperationInfo(sample);
            // make the event available to EventProcessors
            ringBuffer.publish(sequence);
            LOGGER.info(String.format("published : %s:%s:%d, %d, %d",
                    scenario.getName(), scenario.getTableName(),
                    numOperations, ringBuffer.getCursor(), sequence));
        }
    }

    @Override public void stop() throws Exception {
        // Wait for the handlers to finish the jobs
        if (disruptor != null) {
            disruptor.shutdown();
        }

        // TODO need to handle asynchronous result publishing
    }

    @Override public List<PherfWorkHandler> getWorkHandlers(Properties properties) {

        String handlerName = "";
        try {
            handlerName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        int handlerCount = DEFAULT_NUM_HANDLER_PER_SCENARIO;
        if (properties.containsKey("pherf.mt.handlers_per_scenario")) {
            handlerCount = Integer.parseInt((String)properties.get("pherf.mt.handlers_per_scenario"));
        }
        List<PherfWorkHandler> workers = Lists.newArrayListWithCapacity(handlerCount);
        for (int i = 0; i < handlerCount; i++) {
            String handlerId = String.format("%s.%d", handlerName, i + 1);
            workers.add(new TenantOperationWorkHandler(operationFactory, handlerId));
        }
        return workers;
    }

    abstract public TenantOperationInfo next();


}
