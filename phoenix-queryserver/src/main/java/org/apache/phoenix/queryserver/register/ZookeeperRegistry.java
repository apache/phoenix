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


import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.loadbalancer.service.Instance;
import org.apache.phoenix.query.QueryServices;

import java.io.IOException;
import java.net.InetAddress;

public class ZookeeperRegistry implements Registry {

    private  ServiceDiscovery<Instance> serviceDiscovery;
    private  ServiceInstance<Instance> instance;

    private ZookeeperRegistry(Integer load, CuratorFramework client, String path,
                             String serviceName,Integer port) throws Exception{

        String uri = String.format("%s://%s:%s","http", InetAddress.getLocalHost().getHostName(),port);
        UriSpec uriSpec = new UriSpec(uri);

        instance = ServiceInstance.<Instance>builder()
                .name(serviceName)
                .payload(new Instance(load))
                .port(port) // in a real application, you'd use a common port
                .uriSpec(uriSpec)
                .build();

        // if you mark your payload class with @JsonRootName the provided JsonInstanceSerializer will work
        JsonInstanceSerializer<Instance> serializer = new JsonInstanceSerializer<>(Instance.class);

        serviceDiscovery = ServiceDiscoveryBuilder.builder(Instance.class)
                .client(client)
                .basePath(path)
                .serializer(serializer)
                .thisInstance(instance)
                .build();

    }

    public ZookeeperRegistry(){}

    @Override
    public void start() throws Exception {
        serviceDiscovery.start();
    }

    @Override
    public void close() throws IOException {
        CloseableUtils.closeQuietly(serviceDiscovery);
    }

    @Override
    public Registry registerYourself(Integer load,  String path,
                                              String serviceName, Integer port, Configuration configuration) throws Exception {

        String zookeeperQuorum=configuration.get(QueryServices.ZOOKEEPER_QUORUM_ATTRIB);
        String zookeeperPort=configuration.get(QueryServices.ZOOKEEPER_PORT_ATTRIB);
        final CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(String.format("%s:%s",zookeeperQuorum,zookeeperPort), new ExponentialBackoffRetry(1000, 3));
        return  new ZookeeperRegistry(load, curatorFramework, path, serviceName, port);
    }
}
