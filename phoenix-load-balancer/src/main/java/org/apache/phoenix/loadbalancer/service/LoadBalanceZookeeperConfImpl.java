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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

import java.util.Arrays;
import java.util.List;



public class LoadBalanceZookeeperConfImpl implements LoadBalanceZookeeperConf {

        private Configuration configuration;

        public LoadBalanceZookeeperConfImpl() {
           this.configuration = HBaseConfiguration.create();
        }

        public LoadBalanceZookeeperConfImpl(Configuration configuration) {
            this.configuration = configuration;
        }

        @VisibleForTesting
        public void setConfiguration(Configuration configuration) {
            this.configuration = configuration;
        }

        @Override
        public String getQueryServerBasePath(){
            return configuration.get(QueryServices.PHOENIX_QUERY_SERVER_CLUSTER_BASE_PATH,
                    QueryServicesOptions.DEFAULT_PHOENIX_QUERY_SERVER_CLUSTER_BASE_PATH);
        }

        @Override
        public String getServiceName(){
            return configuration.get(QueryServices.PHOENIX_QUERY_SERVER_SERVICE_NAME,
                    QueryServicesOptions.DEFAULT_PHOENIX_QUERY_SERVER_SERVICE_NAME);
        }

        @Override
        public String getZkConnectString(){
            return String.format("%s:%s",configuration.get(QueryServices.ZOOKEEPER_QUORUM_ATTRIB,
                    "localhost"),configuration.get(QueryServices.ZOOKEEPER_PORT_ATTRIB,"2181"));
        }

        private String getZkLbUserName(){
            return configuration.get(QueryServices.PHOENIX_QUERY_SERVER_ZK_ACL_USERNAME,
                    QueryServicesOptions.DEFAULT_PHOENIX_QUERY_SERVER_ZK_ACL_USERNAME);
        }

        private String getZkLbPassword(){
            return configuration.get(QueryServices.PHOENIX_QUERY_SERVER_ZK_ACL_PASSWORD,
                    QueryServicesOptions.DEFAULT_PHOENIX_QUERY_SERVER_ZK_ACL_PASSWORD);
        }

        @Override
        public List<ACL> getAcls() {
            ACL acl = new ACL();
            acl.setId(new Id("digest",getZkLbUserName()+":"+getZkLbPassword()));
            acl.setPerms(ZooDefs.Perms.READ);
            return Arrays.asList(acl);
        }

        @Override
        public String getParentPath() {
            String path = String.format("%s/%s",getQueryServerBasePath(),getServiceName());
            return path;
        }

        @Override
        public String getFullPathToNode(HostAndPort hostAndPort) {
            String path = String.format("%s/%s",getParentPath()
                    ,hostAndPort.toString());
            return path;
        }
}

