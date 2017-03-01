/**
 *
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
package org.apache.phoenix.queryserver.register;


import com.google.common.net.HostAndPort;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.phoenix.loadbalancer.service.LoadBalanceZookeeperConf;
import org.apache.phoenix.queryserver.server.QueryServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;


public class ZookeeperRegistry implements Registry {

    private static final Log LOG = LogFactory.getLog(ZookeeperRegistry.class);
    private CuratorFramework client;

    public ZookeeperRegistry(){}

    @Override
    public  void registerServer(LoadBalanceZookeeperConf configuration, int pqsPort,
                                String zookeeperConnectString, String pqsHost)
            throws Exception {

        this.client = CuratorFrameworkFactory.newClient(zookeeperConnectString,
                new ExponentialBackoffRetry(1000,10));
        this.client.start();
        HostAndPort hostAndPort = HostAndPort.fromParts(pqsHost,pqsPort);
        String path = configuration.getFullPathToNode(hostAndPort);
        String node = hostAndPort.toString();
        this.client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path
                ,node.getBytes(StandardCharsets.UTF_8));
        Stat stat = this.client.setACL().withACL(configuration.getAcls()).forPath(path);
        if (stat != null) {
            LOG.info(" node created with right ACL");
        }
        else {
            LOG.error("could not create node with right ACL. So, system would exit now.");
            throw new RuntimeException(" Unable to connect to Zookeeper");
        }

    }

    @Override
    public void unRegisterServer() throws Exception {
        CloseableUtils.closeQuietly(this.client);
    }
}
