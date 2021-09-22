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

import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.ExecutionType;
import org.apache.phoenix.pherf.configuration.LoadProfile;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.configuration.TenantGroup;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.mt.handlers.PherfWorkHandler;
import org.apache.phoenix.pherf.workload.mt.handlers.RendezvousingWorkHandler;
import org.apache.phoenix.pherf.workload.mt.operations.Operation;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CyclicBarrier;

/**
 * A load generator that generates tenant operation events in the order specified in the
 * scenario file.
 * The scenario file can also specify on how many iterations to be executed per operation,
 * whether the iterations be run in parallel or serially.
 */
public class SequentialLoadEventGenerator extends BaseLoadEventGenerator {

    private static class SequentialSampler {
        private final LoadProfile loadProfile;
        private final String modelName;
        private final String scenarioName;
        private final String tableName;
        private long iteration;
        private int opIndex;
        private int numHandlers;

        private final TenantGroup tenantGroup;
        private final List<Operation> operationList;

        public SequentialSampler(List<Operation> operationList, DataModel model,
                Scenario scenario, Properties properties) {
            this.modelName = model.getName();
            this.scenarioName = scenario.getName();
            this.tableName = scenario.getTableName();
            this.loadProfile = scenario.getLoadProfile();
            this.operationList = operationList;

            // Track the individual tenant group with single tenant or global connection,
            // so that given a generated sample we can use the supplied tenant.
            // NOTE : Not sure if there is a case for multiple tenants in a uniform distribution.
            // For now keeping it simple.
            Preconditions.checkArgument(loadProfile.getTenantDistribution() != null,
                    "Tenant distribution cannot be null");
            Preconditions.checkArgument(!loadProfile.getTenantDistribution().isEmpty(),
                    "Tenant group cannot be empty");
            Preconditions.checkArgument(loadProfile.getTenantDistribution().size() == 1,
                    "Tenant group cannot be more than 1");
            tenantGroup = loadProfile.getTenantDistribution().get(0);
        }

        public TenantOperationInfo nextSample() {
            Operation op = operationList.get(opIndex % operationList.size());
            String tenantGroupId = tenantGroup.getId();
            String tenantIdPrefix = Strings
                    .padStart(tenantGroupId, loadProfile.getGroupIdLength(), 'x');
            String formattedTenantId = String.format(loadProfile.getTenantIdFormat(),
                    tenantIdPrefix.substring(0, loadProfile.getGroupIdLength()), 1);
            String paddedTenantId = Strings.padStart(formattedTenantId, loadProfile.getTenantIdLength(), 'x');
            String tenantId = paddedTenantId.substring(0, loadProfile.getTenantIdLength());

            TenantOperationInfo sample = new TenantOperationInfo(modelName, scenarioName, tableName,
                    tenantGroupId, op.getId(), tenantId, op);

            iteration++;
            if (iteration % numHandlers == 0) {
                opIndex++;
            }
            return sample;
        }

        public int getNumHandlers() {
            return numHandlers;
        }

        public void setNumHandlers(int handlers) {
            numHandlers = handlers;
        }

    }

    protected static final int DEFAULT_NUM_ITERATIONS = 1;
    protected static final ExecutionType DEFAULT_EXECUTION_TYPE = ExecutionType.SERIAL;
    private final SequentialSampler sampler;
    private int numHandlers;
    private int numIterations = DEFAULT_NUM_ITERATIONS;
    private ExecutionType executionType = DEFAULT_EXECUTION_TYPE;

    public SequentialLoadEventGenerator(PhoenixUtil phoenixUtil, DataModel model, Scenario scenario,
            Properties properties) {
        super(phoenixUtil, model, scenario, properties);
        this.sampler = new SequentialSampler(operationFactory.getOperations(), model, scenario, properties);
        this.sampler.setNumHandlers(this.numHandlers);
    }

    public SequentialLoadEventGenerator(PhoenixUtil phoenixUtil, DataModel model, Scenario scenario,
            List<PherfWorkHandler> workHandlers, Properties properties) {
        super(phoenixUtil, model, scenario, workHandlers, properties);
        this.sampler = new SequentialSampler(operationFactory.getOperations(), model, scenario, properties);
        this.sampler.setNumHandlers(this.numHandlers);
    }

    public List<PherfWorkHandler> getWorkHandlers(Properties properties) {

        String handlerName = "";
        try {
            handlerName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        this.numHandlers = DEFAULT_NUM_HANDLER_PER_SCENARIO;
        if (properties.containsKey(PherfConstants.HANDLERS_PER_SCENARIO_PROP_KEY)) {
            this.numHandlers = Integer.parseInt((String)properties.get(PherfConstants.HANDLERS_PER_SCENARIO_PROP_KEY));
        }

        if (properties.containsKey(PherfConstants.NUM_SEQUENTIAL_ITERATIONS_PROP_KEY)) {
            this.numIterations = Integer.parseInt((String)properties.get(PherfConstants.NUM_SEQUENTIAL_ITERATIONS_PROP_KEY));
        }

        if (properties.containsKey(PherfConstants.NUM_SEQUENTIAL_EXECUTION_TYPE_PROP_KEY)) {
            this.executionType = ExecutionType.valueOf((String)properties.get(PherfConstants.NUM_SEQUENTIAL_EXECUTION_TYPE_PROP_KEY));
            switch (executionType) {
            case SERIAL:
                this.numHandlers = DEFAULT_NUM_ITERATIONS;
                break;
            case PARALLEL:
                this.numHandlers = numIterations;
                break;
            default:
                // Just accepts the defaults, nothing to do here
            }
        }

        Map<String, CyclicBarrier> rendezvousPoints = Maps.newHashMap();
        CyclicBarrier startBarrier = new CyclicBarrier(numHandlers, new Runnable() {
            @Override public void run() {
                LOGGER.info("Rendezvoused for start of operation execution");
            }
        });
        rendezvousPoints.put(PherfConstants.MT_HANDLER_START_RENDEZVOUS_PROP_KEY, startBarrier);

        List<PherfWorkHandler> workers = Lists.newArrayListWithCapacity(numHandlers);
        for (int i = 0; i < numHandlers; i++) {
            String handlerId = String.format("%s.%d", handlerName, i + 1);
            workers.add(new RendezvousingWorkHandler(operationFactory, handlerId,
                    rendezvousPoints));
        }
        return workers;
    }

    @Override public TenantOperationInfo next() {
        return this.sampler.nextSample();
    }
}
