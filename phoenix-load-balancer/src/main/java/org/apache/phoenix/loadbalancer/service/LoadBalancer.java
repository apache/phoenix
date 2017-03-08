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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.loadbalancer.zookeeper.ZookeeperServiceDiscoverer;
import org.apache.phoenix.query.QueryServices;

public class LoadBalancer {

    protected static final Log LOG = LogFactory.getLog(LoadBalancer.class);
    private static Configuration configuration;
    private static ZookeeperServiceDiscoverer zookeeperServiceDiscoverer ;
    private static LoadBalancer loadBalancer;
    private static final String connectString;
    private static String basePath ;
    private static String serviceName;

    static {
        configuration = HBaseConfiguration.create();
        String zookeeperQuorum=configuration.get(QueryServices.ZOOKEEPER_QUORUM_ATTRIB);
        String zookeeperPort=configuration.get(QueryServices.ZOOKEEPER_PORT_ATTRIB);
        connectString = String.format("%s:%s",zookeeperQuorum,zookeeperPort);
        basePath=configuration.get(QueryServices.PHOENIX_QUERYSERVER_BASE_PATH);
        serviceName=configuration.get(QueryServices.PHOENIX_QUERYSERVER_SERVICENAME);
        try {
            loadBalancer = new LoadBalancer();
        } catch (Exception e) {
            LOG.error(" error while initializing load balancer ",e);
        }
    }
    private LoadBalancer() throws Exception{
        try  {
            zookeeperServiceDiscoverer = new ZookeeperServiceDiscoverer
                    (connectString, new ExponentialBackoffRetry(1000, 3),
                            basePath,serviceName);
        }catch(Exception ex){
            LOG.error("Exception while creating a zookeeper service discoverer",ex);
            throw ex;
        }
    }


    public static LoadBalancer getLoadBalancer() {
       return loadBalancer;
    }


    public ServiceDiscoverer getServiceDiscoverer() throws Exception{
        return zookeeperServiceDiscoverer;
    }

}
