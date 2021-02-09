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

import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.mt.handlers.PherfWorkHandler;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.LoadProfile;
import org.apache.phoenix.pherf.configuration.OperationGroup;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.configuration.TenantGroup;
import org.apache.phoenix.pherf.workload.mt.operations.Operation;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * A perf load event generator based on the supplied load profile.
 * The load profile enumerates the distribution of operation among the different tenant group
 * which is used by the load generator to generate the operation events for the various tenants.
 */

public class WeightedRandomLoadEventGenerator extends BaseLoadEventGenerator {

    private static class WeightedRandomSampler {
        private static String AUTO_WEIGHTED_OPERATION_ID = "xxxxxx";
        private final Random RANDOM = new Random();
        private final LoadProfile loadProfile;
        private final String modelName;
        private final String scenarioName;
        private final String tableName;
        private final EnumeratedDistribution<String> distribution;

        private final Map<String, TenantGroup> tenantGroupMap = Maps.newHashMap();
        private final Map<String, Operation> operationMap = Maps.newHashMap();
        private final List<String> autoWeightedOperations = Lists.newArrayList();
        private final int numAutoWeightedOperations;

        public WeightedRandomSampler(List<Operation> operationList, DataModel model, Scenario scenario) {
            this.modelName = model.getName();
            this.scenarioName = scenario.getName();
            this.tableName = scenario.getTableName();
            this.loadProfile = scenario.getLoadProfile();

            // Track the individual tenant group sizes,
            // so that given a generated sample we can get a random tenant for a group.
            for (TenantGroup tg : loadProfile.getTenantDistribution()) {
                tenantGroupMap.put(tg.getId(), tg);
            }
            Preconditions.checkArgument(!tenantGroupMap.isEmpty(),
                    "Tenant group cannot be empty");

            for (Operation op : operationList) {
                for (OperationGroup loadOp : loadProfile.getOpDistribution()) {
                    if (op.getId().compareTo(loadOp.getId()) == 0) {
                        operationMap.put(op.getId(), op);
                    }
                }
            }
            Preconditions.checkArgument(!operationMap.isEmpty(),
                    "Operation list and load profile operation do not match");
            this.distribution = initProbabilityDistribution(scenario.getLoadProfile());
            this.numAutoWeightedOperations = autoWeightedOperations.size();

        }

        public TenantOperationInfo nextSample() {
            String sampleIndex = this.distribution.sample();
            String[] parts = sampleIndex.split(":");
            String tenantGroupId = parts[0];
            String opId = parts[1];

            Operation op = operationMap.get(opId);
            if (op == null && opId.compareTo(AUTO_WEIGHTED_OPERATION_ID) == 0) {
                opId = autoWeightedOperations.get(RANDOM.nextInt(numAutoWeightedOperations));
                op = operationMap.get(opId);
            }
            int numTenants = tenantGroupMap.get(tenantGroupId).getNumTenants();
            String tenantIdPrefix = Strings.padStart(tenantGroupId, loadProfile.getGroupIdLength(), 'x');
            String formattedTenantId = String.format(loadProfile.getTenantIdFormat(),
                    tenantIdPrefix.substring(0, loadProfile.getGroupIdLength()), RANDOM.nextInt(numTenants));
            String paddedTenantId = Strings.padStart(formattedTenantId, loadProfile.getTenantIdLength(), 'x');
            String tenantId = paddedTenantId.substring(0, loadProfile.getTenantIdLength());

            TenantOperationInfo sample = new TenantOperationInfo(modelName, scenarioName, tableName,
                    tenantGroupId, opId, tenantId, op);
            return sample;
        }

        private EnumeratedDistribution initProbabilityDistribution(LoadProfile loadProfile) {
            double totalTenantGroupWeight = 0.0f;
            double totalOperationWeight = 0.0f;
            double remainingOperationWeight = 0.0f;

            // Sum the weights to find the total weight,
            // so that the weights can be used in the total probability distribution.
            for (TenantGroup tg : loadProfile.getTenantDistribution()) {
                Preconditions.checkArgument(tg.getWeight() > 0.0f,
                        "Tenant group weight cannot be less than zero");
                totalTenantGroupWeight += tg.getWeight();
            }
            for (OperationGroup op : loadProfile.getOpDistribution()) {
                if (op.getWeight() > 0.0f) {
                    totalOperationWeight += op.getWeight();
                } else {
                    autoWeightedOperations.add(op.getId());
                }
            }

            if (!autoWeightedOperations.isEmpty()) {
                remainingOperationWeight = 100.0f - totalOperationWeight;
                totalOperationWeight = 100.0f;
            }

            Preconditions.checkArgument(totalTenantGroupWeight == 100.0f,
                    "Total tenant group weight cannot be <> 100.0");
            Preconditions.checkArgument(totalOperationWeight == 100.0f,
                    "Total operation group weight cannot be <> 100.0");

            // Initialize the sample probability distribution
            List<Pair<String, Double>> pmf = Lists.newArrayList();
            double totalWeight = totalTenantGroupWeight * totalOperationWeight;
            for (TenantGroup tg : loadProfile.getTenantDistribution()) {
                for (OperationGroup op : loadProfile.getOpDistribution()) {
                    int opWeight = op.getWeight();
                    if (opWeight > 0.0f) {
                        String sampleName = String.format("%s:%s", tg.getId(), op.getId());
                        double probability = (tg.getWeight() * opWeight)/totalWeight;
                        pmf.add(new Pair(sampleName, probability));
                    }
                }

                if (!autoWeightedOperations.isEmpty()) {
                    String sampleName = String.format("%s:%s", tg.getId(), AUTO_WEIGHTED_OPERATION_ID);
                    double probability = (tg.getWeight() * remainingOperationWeight)/totalWeight;
                    pmf.add(new Pair(sampleName, probability));
                }
            }
            EnumeratedDistribution distribution = new EnumeratedDistribution(pmf);
            return distribution;
        }
    }

    private final WeightedRandomSampler sampler;

    public WeightedRandomLoadEventGenerator(PhoenixUtil phoenixUtil, DataModel model, Scenario scenario,
            Properties properties) {
        super(phoenixUtil, model, scenario, properties);
        this.sampler = new WeightedRandomSampler(operationFactory.getOperations(), model, scenario);
    }

    public WeightedRandomLoadEventGenerator(PhoenixUtil phoenixUtil, DataModel model, Scenario scenario,
            List<PherfWorkHandler> workHandlers, Properties properties) {
        super(phoenixUtil, model, scenario, workHandlers, properties);
        this.sampler = new WeightedRandomSampler(operationFactory.getOperations(), model, scenario);
    }

    @Override public TenantOperationInfo next() {
        return this.sampler.nextSample();
    }

}
