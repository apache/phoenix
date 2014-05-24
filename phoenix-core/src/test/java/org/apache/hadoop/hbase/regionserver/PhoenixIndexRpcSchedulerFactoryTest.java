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
import org.junit.Test;

public class PhoenixIndexRpcSchedulerFactoryTest {

    @Test
    public void ensureInstantiation() throws Exception {
        Configuration conf = new Configuration(false);
        conf.setClass(HRegionServer.REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS,
            PhoenixIndexRpcSchedulerFactory.class, RpcSchedulerFactory.class);
        // kinda lame that we copy the copy from the regionserver to do this and can't use a static
        // method, but meh
        try {
            Class<?> rpcSchedulerFactoryClass =
                    conf.getClass(HRegionServer.REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS,
                        SimpleRpcSchedulerFactory.class);
            Object o = ((RpcSchedulerFactory) rpcSchedulerFactoryClass.newInstance());
            assertTrue(o instanceof PhoenixIndexRpcSchedulerFactory);
        } catch (InstantiationException e) {
            assertTrue("Should not have got an exception when instantiing the rpc scheduler: " + e,
                false);
        } catch (IllegalAccessException e) {
            assertTrue("Should not have got an exception when instantiing the rpc scheduler: " + e,
                false);
        }
    }

    /**
     * Ensure that we can't configure the index priority ranges inside the hbase ranges
     * @throws Exception
     */
    @Test
    public void testValidateIndexPriorityRanges() throws Exception {
        Configuration conf = new Configuration(false);
        // standard configs should be fine
        PhoenixIndexRpcSchedulerFactory factory = new PhoenixIndexRpcSchedulerFactory();
        factory.create(conf, null);

        setMinMax(conf, 0, 4);
        factory.create(conf, null);

        setMinMax(conf, 101, 102);
        factory.create(conf, null);

        setMinMax(conf, 102, 101);
        try {
            factory.create(conf, null);
            fail("Should not have allowed max less than min");
        } catch (IllegalArgumentException e) {
            // expected
        }

        setMinMax(conf, 5, 6);
        try {
            factory.create(conf, null);
            fail("Should not have allowed min in range");
        } catch (IllegalArgumentException e) {
            // expected
        }

        setMinMax(conf, 6, 60);
        try {
            factory.create(conf, null);
            fail("Should not have allowed min/max in hbase range");
        } catch (IllegalArgumentException e) {
            // expected
        }

        setMinMax(conf, 6, 101);
        try {
            factory.create(conf, null);
            fail("Should not have allowed in range");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    private void setMinMax(Configuration conf, int min, int max) {
        conf.setInt(PhoenixIndexRpcSchedulerFactory.MIN_INDEX_PRIOIRTY_KEY, min);
        conf.setInt(PhoenixIndexRpcSchedulerFactory.MAX_INDEX_PRIOIRTY_KEY, max);
    }
}