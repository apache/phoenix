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

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;
import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.LoadProfile;
import org.apache.phoenix.pherf.configuration.OperationGroup;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.configuration.TenantGroup;
import org.apache.phoenix.pherf.workload.mt.Operation;
import org.apache.phoenix.pherf.workload.mt.EventGenerator;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * A perf load event generator based on the supplied load profile.
 */

public class TenantOperationEventGenerator
        implements EventGenerator<TenantOperationInfo> {

    private static class WeightedRandomSampler {
        private final Random RANDOM = new Random();
        private final LoadProfile loadProfile;
        private final String modelName;
        private final String scenarioName;
        private final String tableName;
        private final EnumeratedDistribution<String> distribution;

        private final Map<String, TenantGroup> tenantGroupMap = Maps.newHashMap();
        private final Map<String, Operation> operationMap = Maps.newHashMap();
        private final Map<String, OperationGroup> operationGroupMap = Maps.newHashMap();

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
                for (OperationGroup og : loadProfile.getOpDistribution()) {
                    if (op.getId().compareTo(og.getId()) == 0) {
                        operationMap.put(op.getId(), op);
                        operationGroupMap.put(op.getId(), og);
                    }
                }
            }
            Preconditions.checkArgument(!operationMap.isEmpty(),
                    "Operation list and load profile operation do not match");

            double totalTenantGroupWeight = 0.0f;
            double totalOperationGroupWeight = 0.0f;
            // Sum the weights to find the total weight,
            // so that the weights can be used in the total probability distribution.
            for (TenantGroup tg : loadProfile.getTenantDistribution()) {
                totalTenantGroupWeight += tg.getWeight();
            }
            for (OperationGroup og : loadProfile.getOpDistribution()) {
                totalOperationGroupWeight += og.getWeight();
            }

            Preconditions.checkArgument(totalTenantGroupWeight != 0.0f,
                    "Total tenant group weight cannot be zero");
            Preconditions.checkArgument(totalOperationGroupWeight != 0.0f,
                    "Total operation group weight cannot be zero");

            // Initialize the sample probability distribution
            List<Pair<String, Double>> pmf = Lists.newArrayList();
            double totalWeight = totalTenantGroupWeight * totalOperationGroupWeight;
            for (TenantGroup tg : loadProfile.getTenantDistribution()) {
                for (String opId : operationMap.keySet()) {
                    String sampleName = String.format("%s:%s", tg.getId(), opId);
                    int opWeight = operationGroupMap.get(opId).getWeight();
                    double probability = (tg.getWeight() * opWeight)/totalWeight;
                    pmf.add(new Pair(sampleName, probability));
                }
            }
            this.distribution = new EnumeratedDistribution(pmf);
        }

        public TenantOperationInfo nextSample() {
            String sampleIndex = this.distribution.sample();
            String[] parts = sampleIndex.split(":");
            String tenantGroupId = parts[0];
            String opId = parts[1];

            Operation op = operationMap.get(opId);
            int numTenants = tenantGroupMap.get(tenantGroupId).getNumTenants();
            String tenantIdPrefix = Strings.padStart(tenantGroupId, loadProfile.getGroupIdLength(), '0');
            String formattedTenantId = String.format(loadProfile.getTenantIdFormat(),
                    tenantIdPrefix.substring(0, loadProfile.getGroupIdLength()), RANDOM.nextInt(numTenants));
            String paddedTenantId = Strings.padStart(formattedTenantId, loadProfile.getTenantIdLength(), '0');
            String tenantId = paddedTenantId.substring(0, loadProfile.getTenantIdLength());

            TenantOperationInfo sample = new TenantOperationInfo(modelName, scenarioName, tableName,
                    tenantGroupId, opId, tenantId, op);
            return sample;
        }
    }

    private final WeightedRandomSampler sampler;
    private final Properties properties;

    public TenantOperationEventGenerator(List<Operation> ops, DataModel model, Scenario scenario)
            throws Exception {
        this(ops, model, scenario,
                PherfConstants.create().getProperties(PherfConstants.PHERF_PROPERTIES, true));
    }

    public TenantOperationEventGenerator(List<Operation> ops, DataModel model, Scenario scenario,
            Properties properties) {
        this.properties = properties;
        this.sampler = new WeightedRandomSampler(ops, model, scenario);
    }

    @Override public TenantOperationInfo next() {
        return this.sampler.nextSample();
    }
}
