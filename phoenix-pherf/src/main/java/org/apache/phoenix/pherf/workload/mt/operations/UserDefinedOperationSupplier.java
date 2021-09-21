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

import org.apache.phoenix.pherf.workload.mt.generators.TenantOperationInfo;
import org.apache.phoenix.thirdparty.com.google.common.base.Function;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.EnvironmentEdgeManager;

/**
 * A supplier of {@link Function} that takes {@link UserDefinedOperation} as an input
 */
public class UserDefinedOperationSupplier extends BaseOperationSupplier {

    public UserDefinedOperationSupplier(PhoenixUtil phoenixUtil, DataModel model, Scenario scenario) {
        super(phoenixUtil, model, scenario);
    }

    @Override
    public Function<TenantOperationInfo, OperationStats> get() {
        return new Function<TenantOperationInfo, OperationStats>() {
            @Override
            public OperationStats apply(final TenantOperationInfo input) {
                Preconditions.checkNotNull(input);
                // TODO : implement user defined operation invocation.
                long startTime = EnvironmentEdgeManager.currentTimeMillis();
                long duration = EnvironmentEdgeManager.currentTimeMillis() - startTime;
                return new OperationStats(input, startTime,0, 0, duration);
            }
        };
    }
}
