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
package org.apache.phoenix.loadbalancer.zookeeper;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.phoenix.loadbalancer.service.Instance;
import org.apache.phoenix.loadbalancer.service.LoadBalancer;
import org.apache.phoenix.loadbalancer.service.ServiceDiscoverer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Collection;


public class ZookeeperServiceDiscoverer implements ServiceDiscoverer {


    private String basePath ;
    private String serviceName;
    private CuratorFramework client;
    private ServiceDiscovery<Instance> serviceDiscovery;
    private final List<Closeable> closeAbles = Lists.newArrayList();
    protected static final Log LOG = LogFactory.getLog(LoadBalancer.class);
    private static ArrayList<Instance> services = Lists.newArrayList();

    public ZookeeperServiceDiscoverer(String zookeeperConnectString, RetryPolicy retryPolicy,
                                      String basePath, String serviceName) throws Exception {

        this.basePath = basePath;
        this.serviceName = serviceName;
        client = CuratorFrameworkFactory.newClient(zookeeperConnectString, retryPolicy);
        client.start();
        JsonInstanceSerializer<Instance> serializer = new JsonInstanceSerializer<>(Instance.class);
        serviceDiscovery = ServiceDiscoveryBuilder.builder(Instance.class).client(client)
                .basePath(this.basePath).serializer(serializer).build();
        serviceDiscovery.start();
        closeAbles.add(client);
        closeAbles.add(serviceDiscovery);
    }

    @Override
    public Instance getServiceLocation() {
        this.refreshServiceLocationList();
        Collections.sort(services, new Comparator<Instance>() {
            @Override
            public int compare(Instance o1, Instance o2) {
                if (o1.getLoad() == o2.getLoad() ) return 0;
                if (o1.getLoad() > o2.getLoad() ) return 1;
                return -1;
            }
        });
        return refreshServiceLocationList().get(0);
    }

    @Override
    public void close() throws IOException {
        for (Closeable closeable : closeAbles) {
            if (closeable!=null) closeable.close();
        }
    }

    private List<Instance> refreshServiceLocationList() {
        services.clear();
        try
        {
            Collection<ServiceInstance<Instance>> serviceInstances = serviceDiscovery.queryForInstances(serviceName);
            for(ServiceInstance<Instance> node:serviceInstances) {
                services.add(node.getPayload());
            }
        }
        finally
        {
           return services;
        }
    }

}
