/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.monitoring;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.InstanceResolver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.hamcrest.CoreMatchers;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.DriverManager;
import java.util.Map;

import static org.apache.phoenix.monitoring.NoOpGlobalMetricImpl.NO_SAMPLES;
import static org.apache.phoenix.monitoring.NoOpGlobalMetricImpl.NO_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@Category(NeedsOwnMiniClusterTest.class)
public class PhoenixMetricsDisabledIT extends BaseTest {

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        final Configuration conf = HBaseConfiguration.create();
        conf.set(QueryServices.GLOBAL_METRICS_ENABLED, String.valueOf(false));
        conf.set(QueryServices.RENEW_LEASE_ENABLED, String.valueOf(false));
        // Clear the cached singletons so we can inject our own.
        InstanceResolver.clearSingletons();
        // Make sure the ConnectionInfo doesn't try to pull a default Configuration
        InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {
            @Override
            public Configuration getConfiguration() {
                return conf;
            }
            @Override
            public Configuration getConfiguration(Configuration confToClone) {
                Configuration copy = new Configuration(conf);
                copy.addResource(confToClone);
                return copy;
            }
        });

        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));

        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
    }

    @Test
    public void testResetGlobalPhoenixMetrics() {
        for (GlobalMetric m : PhoenixRuntime.getGlobalPhoenixClientMetrics()) {
            assertThat(m, CoreMatchers.<GlobalMetric>instanceOf(NoOpGlobalMetricImpl.class));
            assertEquals(NO_VALUE, m.getValue());
            assertEquals(NO_SAMPLES, m.getNumberOfSamples());
        }
    }

}
