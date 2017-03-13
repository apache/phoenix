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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.phoenix.loadbalancer.service.Instance;
import org.apache.phoenix.queryserver.server.QueryServer;

import java.io.IOException;
import java.net.InetAddress;

public class ZookeeperRegistry implements Registry {

    protected static final Log LOG = LogFactory.getLog(QueryServer.class);
    private  ServiceDiscovery<Instance> serviceDiscovery;
    private  ServiceInstance<Instance> avanticaInstance;
    private CuratorFramework client;

    private void register(Integer load,  String path,
                             String serviceName,Integer avaticaServerPort) throws Exception{

        String uri = String.format("%s://%s:%s","http", InetAddress.getLocalHost().getHostName(),avaticaServerPort);
        UriSpec uriSpec = new UriSpec(uri);

        avanticaInstance = ServiceInstance.<Instance>builder()
                .name(serviceName)
                .payload(new Instance(load))
                .port(avaticaServerPort)
                .uriSpec(uriSpec)
                .build();

        JsonInstanceSerializer<Instance> serializer = new JsonInstanceSerializer<>(Instance.class);

        serviceDiscovery = ServiceDiscoveryBuilder.builder(Instance.class)
                .client(client)
                .basePath(path)
                .serializer(serializer)
                .thisInstance(avanticaInstance)
                .build();

    }

    public ZookeeperRegistry(){}

    @Override
    public void start() throws Exception {
        if (client != null)
            client.start();
        else {
            LOG.error(" zookeeper client service could not be initialized. ");
            throw new Exception(" zookeeper client service could not be initialized ");
        }
        if (serviceDiscovery != null)
                serviceDiscovery.start();
        else {
            LOG.error(" zookeeper service could not be initialized. ");
            throw new Exception(" zookeeper service could not be initialized ");
        }
    }

    @Override
    public void close() throws IOException {
        CloseableUtils.closeQuietly(serviceDiscovery);
        CloseableUtils.closeQuietly(client);
    }

    @Override
    public  void registerServer(Integer load, String path,
                                   String serviceName, Integer avaticaServerPort, String zookeeperConnectString)
            throws Exception {

        this.client = CuratorFrameworkFactory.newClient(zookeeperConnectString,
                new ExponentialBackoffRetry(1000, 3));
         this.register(load, path, serviceName, avaticaServerPort);
    }
}
