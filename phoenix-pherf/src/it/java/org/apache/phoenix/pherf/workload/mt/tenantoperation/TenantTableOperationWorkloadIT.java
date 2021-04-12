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

import com.lmax.disruptor.WorkHandler;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests focused on tenant tablee operations and their validations
 * Tests focused on tenant operation workloads {@link TenantOperationWorkload}
 * and workload handlers {@link WorkHandler}
 */
public class TenantTableOperationWorkloadIT extends MultiTenantTableOperationBaseIT {
    private final MultiTenantTestUtils multiTenantTestUtils = new MultiTenantTestUtils();
    private final DataModel model;

    public TenantTableOperationWorkloadIT() throws Exception {
        model = readTestDataModel("/scenario/test_tbl_workload.xml");
    }

    @Test public void testVariousOperations() throws Exception {
        int expectedTenantGroups = 1;
        int expectedWriteOpGroups = 1;
        int expectedReadOpGroups = 2;
        multiTenantTestUtils.testVariousOperations(properties, model, "TEST_TABLE_WRITE",
                expectedTenantGroups, expectedWriteOpGroups);
        multiTenantTestUtils.testVariousOperations(properties, model, "TEST_TABLE_READ",
                expectedTenantGroups, expectedReadOpGroups);
    }

    @Test public void testWorkloadWithOneHandler() throws Exception {
        int expectedTenantGroups = 1;
        int expectedWriteOpGroups = 1;
        int expectedReadOpGroups = 2;
        multiTenantTestUtils.testWorkloadWithOneHandler(properties, model, "TEST_TABLE_WRITE",
                expectedTenantGroups, expectedWriteOpGroups);
        multiTenantTestUtils.testWorkloadWithOneHandler(properties, model, "TEST_TABLE_READ",
                expectedTenantGroups, expectedReadOpGroups);
    }

    @Test public void testWorkloadWithManyHandlers() throws Exception {
        int expectedTenantGroups = 1;
        int expectedWriteOpGroups = 1;
        int expectedReadOpGroups = 2;
        multiTenantTestUtils.testWorkloadWithManyHandlers(properties, model, "TEST_TABLE_WRITE",
                expectedTenantGroups, expectedWriteOpGroups);
        multiTenantTestUtils.testWorkloadWithManyHandlers(properties, model, "TEST_TABLE_READ",
                expectedTenantGroups, expectedReadOpGroups);
    }

}
