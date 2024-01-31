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
package org.apache.phoenix.mapreduce;

import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.apache.phoenix.mapreduce.util.
        PhoenixConfigurationUtil.MAPREDUCE_MULTI_INPUT_MAPPER_SPLIT_SIZE;
import static org.apache.phoenix.mapreduce.util.
        PhoenixConfigurationUtil.MAPREDUCE_MULTI_INPUT_SPLIT_STRATEGY_CLAZZ;
import static org.apache.phoenix.mapreduce.util.
        PhoenixConfigurationUtil.MAPREDUCE_MULTI_INPUT_STRATEGY_CLAZZ;
import static org.apache.phoenix.util.PhoenixRuntime.CONNECTIONLESS;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_ZK;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobContext;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.junit.Test;
import org.mockito.Mockito;


public class PhoenixMultiViewInputFormatTest {

    private static String CONNECTIONLESS_URL =
            JDBC_PROTOCOL_ZK + JDBC_PROTOCOL_SEPARATOR + CONNECTIONLESS + JDBC_PROTOCOL_TERMINATOR
                    + PHOENIX_TEST_DRIVER_URL_PARAM + JDBC_PROTOCOL_TERMINATOR;

    @Test
    public void testDefaultConfig() throws Exception {
        PhoenixMultiViewInputFormat multiViewInputFormat = new PhoenixMultiViewInputFormat();

        Configuration config = new Configuration();
        config.set(MAPREDUCE_MULTI_INPUT_MAPPER_SPLIT_SIZE, "10");
        PhoenixConfigurationUtil.setInputClusterUrl(config, CONNECTIONLESS_URL);
        JobContext mockContext = Mockito.mock(JobContext.class);
        when(mockContext.getConfiguration()).thenReturn(config);

        // default run should not raise error
        multiViewInputFormat.getSplits(mockContext);
    }


    @Test
    public void testCustomizedInputStrategyClassNotExists() {
        PhoenixMultiViewInputFormat multiViewInputFormat = new PhoenixMultiViewInputFormat();

        Configuration config = new Configuration();
        config.set(MAPREDUCE_MULTI_INPUT_MAPPER_SPLIT_SIZE, "10");
        config.set(MAPREDUCE_MULTI_INPUT_STRATEGY_CLAZZ, "dummy.path");
        PhoenixConfigurationUtil.setInputClusterUrl(config, CONNECTIONLESS_URL);
        JobContext mockContext = Mockito.mock(JobContext.class);
        when(mockContext.getConfiguration()).thenReturn(config);

        try {
            multiViewInputFormat.getSplits(mockContext);
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("ClassNotFoundException"));
        }
    }

    @Test
    public void testCustomizedInputSplitClassNotExists() {
        PhoenixMultiViewInputFormat multiViewInputFormat = new PhoenixMultiViewInputFormat();

        Configuration config = new Configuration();
        config.set(MAPREDUCE_MULTI_INPUT_MAPPER_SPLIT_SIZE, "10");
        config.set(MAPREDUCE_MULTI_INPUT_SPLIT_STRATEGY_CLAZZ, "dummy.path");
        PhoenixConfigurationUtil.setInputClusterUrl(config, CONNECTIONLESS_URL);
        JobContext mockContext = Mockito.mock(JobContext.class);
        when(mockContext.getConfiguration()).thenReturn(config);

        try {
            multiViewInputFormat.getSplits(mockContext);
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("ClassNotFoundException"));
        }
    }
}