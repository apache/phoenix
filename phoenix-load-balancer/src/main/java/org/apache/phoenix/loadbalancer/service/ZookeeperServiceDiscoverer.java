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
package org.apache.phoenix.loadbalancer.service;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


@Singleton
public class ZookeeperServiceDiscoverer implements ServiceDiscoverer {


    @Inject
    private String BASE_PATH ;

    @Inject
    private String serviceName;

    private CuratorFramework client;

    private ServiceDiscovery<ZookeeperNode> serviceDiscovery;

    private final List<Closeable> closeAbles = Lists.newArrayList();


    public ZookeeperServiceDiscoverer(String zookeeperConnectString, RetryPolicy retryPolicy) throws Exception {


        client = CuratorFrameworkFactory.newClient(zookeeperConnectString, retryPolicy);
        client.start();
        JsonInstanceSerializer<ZookeeperNode> serializer = new JsonInstanceSerializer<>(ZookeeperNode.class);
        serviceDiscovery = ServiceDiscoveryBuilder.builder(ZookeeperNode.class).client(client)
                .basePath(BASE_PATH).serializer(serializer).build();
        serviceDiscovery.start();
        closeAbles.add(client);
        closeAbles.add(serviceDiscovery);
    }

    @Override
    public List<Instance> getServiceLocationList() throws Exception{
        ArrayList<Instance> services = Lists.newArrayList();
        try
        {
            Collection<ServiceInstance<ZookeeperNode>> serviceInstances = serviceDiscovery.queryForInstances(serviceName);
            services = Lists.newArrayList();
            for(ServiceInstance<ZookeeperNode> instance:serviceInstances) {
                services.add(new ZookeeperNode(instance.getAddress(),instance.getPort()));
            }
        }
        finally
        {
           return services;
        }
    }


    @Override
    public void close() throws IOException {
        for (Closeable closeable : closeAbles) {
            closeable.close();
        }
    }
}
