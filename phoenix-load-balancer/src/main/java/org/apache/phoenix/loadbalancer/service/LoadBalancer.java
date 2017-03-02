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

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.loadbalancer.inject.GuiceModule;

public class LoadBalancer {

    private Injector injector;

    private Configuration configuration;

    @Inject
    private ZookeeperServiceDiscoverer zookeeperServiceDiscoverer;

    private static volatile LoadBalancer loadBalancer= new LoadBalancer();

    private LoadBalancer() {
        injector = Guice.createInjector(new GuiceModule());
        configuration= injector.getInstance(Configuration.class);
    }

    public static LoadBalancer getLoadBalancer() {
        return loadBalancer;
    }

    public static Configuration getConfiguration(){
        return loadBalancer.configuration;
    }

    public ServiceDiscoverer getServiceDiscoverer() {
        return zookeeperServiceDiscoverer;
    }

}
