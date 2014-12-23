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
package org.apache.phoenix;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.ReadOnlyProps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts up a self-contained HBase cluster with Phoenix installed to allow simple local
 * testing of Phoenix.
 */
public class Sandbox {

    private static final Logger LOG = LoggerFactory.getLogger(Sandbox.class);

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Phoenix sandbox");
        Configuration conf = HBaseConfiguration.create();
        BaseTest.setUpConfigForMiniCluster(conf, new ReadOnlyProps(ImmutableMap.<String, String>of()));

        final HBaseTestingUtility testUtil = new HBaseTestingUtility(conf);
        testUtil.startMiniCluster();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    if (testUtil != null) {
                        testUtil.shutdownMiniCluster();
                    }
                } catch (Exception e) {
                    LOG.error("Exception caught when shutting down mini cluster", e);
                }
            }
        });

        int clientPort = testUtil.getZkCluster().getClientPort();
        System.out.println("\n\n\tPhoenix Sandbox is started\n\n");
        System.out.printf("\tYou can now connect with url 'jdbc:phoenix:localhost:%d'\n" +
                        "\tor connect via sqlline with 'bin/sqlline.py localhost:%d'\n\n",
                clientPort, clientPort);

        Thread.sleep(Long.MAX_VALUE);
    }

}
