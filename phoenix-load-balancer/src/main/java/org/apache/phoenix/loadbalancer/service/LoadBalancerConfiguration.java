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



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import java.util.Arrays;
import java.util.List;

/**
 * Basic Configuration specific to Load Balancer only.
 * The instance of {@link LoadBalancerConfiguration} is
 * used as singleton so that we do not instantiate the class
 * many times.
 */
public class LoadBalancerConfiguration {

    public  final static String clusterName = "phoenix.queryserver.base.path";
    public  final static String serviceName = "phoenix.queryserver.service.name";
    public  final static String defaultClusterName = "phoenix";
    public  final static String defaultServiceName = "queryserver";
    public  static String zkQuorum = "hbase.zookeeper.quorum";
    public  static String zkPort = "hbase.zookeeper.property.clientPort";
    public  static String zkLbUserName = "phoenix.queryserver.zookeeper.acl.username";
    public  static String zkLbPassword = "phoenix.queryserver.zookeeper.acl.password";
    public  static String defaultZkLbUserName = "phoenixuser";
    public  static String defaultZkLbPassword = "Xsjdhxsd";
    private static final Configuration configuration = HBaseConfiguration.create();

    public LoadBalancerConfiguration() {}

    public String getQueryServerBasePath(){
        return "/"+configuration.get(clusterName, defaultClusterName);
    }

    public String getServiceName(){
        return configuration.get(serviceName, defaultServiceName);
    }

    public String getZkConnectString(){
        return String.format("%s:%s",configuration.get(zkQuorum,"localhost"),configuration.get(zkPort,"2183"));
    }

    private String getZkLbUserName(){
        return configuration.get(zkLbUserName,defaultZkLbUserName);
    }

    private String getZkLbPassword(){
        return configuration.get(zkLbPassword,defaultZkLbPassword);
    }

    public List<ACL> getAcls() {
        ACL acl = new ACL();
        acl.setId(new Id("digest",getZkLbUserName()+":"+getZkLbPassword()));
        acl.setPerms(ZooDefs.Perms.READ);
        return Arrays.asList(acl);
    }

    public String getParentPath() {
        String path = String.format("%s/%s",getQueryServerBasePath(),getServiceName());
        return path;
    }

    public String getFullPathToNode(String host,String avaticaServerPort) {
        String path = String.format("%s/%s_%s",getParentPath()
                ,host,avaticaServerPort);
        return path;
    }

}
