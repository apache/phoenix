/**
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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.PhoenixRpcSchedulerFactory;
import org.apache.phoenix.query.QueryServices;
import org.junit.Test;

public class PhoenixRpcSchedulerFactoryTest {

    @Test
    public void ensureInstantiation() throws Exception {
        Configuration conf = new Configuration(false);
        conf.setClass(RSRpcServices.REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS,
            PhoenixRpcSchedulerFactory.class, RpcSchedulerFactory.class);
        // kinda lame that we copy the copy from the regionserver to do this and can't use a static
        // method, but meh
        try {
            Class<?> rpcSchedulerFactoryClass =
                    conf.getClass(RSRpcServices.REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS,
                        SimpleRpcSchedulerFactory.class);
            Object o = rpcSchedulerFactoryClass.newInstance();
            assertTrue(o instanceof PhoenixRpcSchedulerFactory);
        } catch (InstantiationException e) {
            assertTrue("Should not have got an exception when instantiing the rpc scheduler: " + e,
                false);
        } catch (IllegalAccessException e) {
            assertTrue("Should not have got an exception when instantiing the rpc scheduler: " + e,
                false);
        }
    }

    /**
     * Ensure that we can't configure the index and metadata priority ranges inside the hbase ranges
     * @throws Exception
     */
    @Test
    public void testValidateRpcPriorityRanges() throws Exception {
        Configuration conf = new Configuration(false);
        // standard configs should be fine
        PhoenixRpcSchedulerFactory factory = new PhoenixRpcSchedulerFactory();
        factory.create(conf, null);

        // test priorities less than HBase range
        setPriorities(conf, -4, -1);
        factory.create(conf, null);

        // test priorities greater than HBase range
        setPriorities(conf, 1001, 1002);
        factory.create(conf, null);

        // test priorities in HBase range
        setPriorities(conf, 1, 201);
        try {
            factory.create(conf, null);
            fail("Should not have allowed priorities in HBase range");
        } catch (IllegalArgumentException e) {
            // expected
        }
        setPriorities(conf, 1001, 1);
        try {
            factory.create(conf, null);
            fail("Should not have allowed priorities in HBase range");
        } catch (IllegalArgumentException e) {
            // expected
        }
        
        // test priorities in HBase range
        setPriorities(conf, 1001, HConstants.NORMAL_QOS);
        try {
            factory.create(conf, null);
            fail("Should not have allowed priorities in HBase range");
        } catch (IllegalArgumentException e) {
            // expected
        }
        setPriorities(conf, HConstants.NORMAL_QOS, 1001);
        try {
            factory.create(conf, null);
            fail("Should not have allowed priorities in HBase range");
        } catch (IllegalArgumentException e) {
            // expected
        }
        
        // test priorities in HBase range
        setPriorities(conf, 1001, HConstants.HIGH_QOS);
        try {
            factory.create(conf, null);
            fail("Should not have allowed priorities in HBase range");
        } catch (IllegalArgumentException e) {
            // expected
        }
        setPriorities(conf, HConstants.HIGH_QOS, 1001);
        try {
            factory.create(conf, null);
            fail("Should not have allowed priorities in HBase range");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    private void setPriorities(Configuration conf, int indexPrioritymin, int metadataPriority) {
        conf.setInt(QueryServices.INDEX_PRIOIRTY_ATTRIB, indexPrioritymin);
        conf.setInt(QueryServices.METADATA_PRIOIRTY_ATTRIB, metadataPriority);
    }
}