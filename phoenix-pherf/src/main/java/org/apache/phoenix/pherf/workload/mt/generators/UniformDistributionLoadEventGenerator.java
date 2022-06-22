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

import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.LoadProfile;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.configuration.TenantGroup;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.mt.operations.Operation;
import org.apache.phoenix.pherf.workload.mt.handlers.PherfWorkHandler;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * A load generator that generates a uniform distribution of operations among the given tenant group.
 */
public class UniformDistributionLoadEventGenerator extends BaseLoadEventGenerator {

    private static class UniformDistributionSampler {

        private final Random RANDOM = new Random();


        private final LoadProfile loadProfile;
        private final String modelName;
        private final String scenarioName;
        private final String tableName;
        private final Random distribution;

        private final TenantGroup tenantGroup;
        private final List<Operation> operationList;

        public UniformDistributionSampler(List<Operation> operationList, DataModel model,
                                          Scenario scenario) {
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

            this.distribution = new Random();
        }

        public TenantOperationInfo nextSample() {
            int sampleIndex = this.distribution.nextInt(operationList.size());
            Operation op = operationList.get(sampleIndex);
            int numTenants = 1;

            if(tenantGroup.getNumTenants() != 0){
                numTenants = tenantGroup.getNumTenants();
            }

            String tenantGroupId = tenantGroup.getId();
            String tenantIdPrefix = Strings
                    .padStart(tenantGroupId, loadProfile.getGroupIdLength(), 'x');
           
            String formattedTenantId = String.format(loadProfile.getTenantIdFormat(),
                    tenantIdPrefix.substring(0, loadProfile.getGroupIdLength()), RANDOM.nextInt(numTenants));

            String paddedTenantId = Strings.padStart(formattedTenantId, loadProfile.getTenantIdLength(), 'x');
            String tenantId = paddedTenantId.substring(0, loadProfile.getTenantIdLength());

            TenantOperationInfo sample = new TenantOperationInfo(modelName, scenarioName, tableName,
                    tenantGroupId, op.getId(), tenantId, op);
            return sample;
        }
    }

    private final UniformDistributionSampler sampler;


    public UniformDistributionLoadEventGenerator(PhoenixUtil phoenixUtil, DataModel model, Scenario scenario,
                                                 Properties properties) {
        super(phoenixUtil, model, scenario, properties);
        this.sampler = new UniformDistributionSampler(operationFactory.getOperations(), model, scenario);
    }

    public UniformDistributionLoadEventGenerator(PhoenixUtil phoenixUtil, DataModel model, Scenario scenario,
                                                 List<PherfWorkHandler> workHandlers, Properties properties) {
        super(phoenixUtil, model, scenario, workHandlers, properties);
        this.sampler = new UniformDistributionSampler(operationFactory.getOperations(), model, scenario);
    }


    @Override public TenantOperationInfo next() {
        return this.sampler.nextSample();
    }
}
