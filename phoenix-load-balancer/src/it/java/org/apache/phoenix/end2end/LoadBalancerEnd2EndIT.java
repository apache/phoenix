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
package org.apache.phoenix.end2end;

import com.google.common.net.HostAndPort;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.phoenix.loadbalancer.service.LoadBalancer;
import org.apache.phoenix.loadbalancer.service.LoadBalanceZookeeperConf;
import org.apache.phoenix.loadbalancer.service.LoadBalanceZookeeperConfImpl;
import org.apache.phoenix.queryserver.register.Registry;
import org.apache.phoenix.queryserver.register.ZookeeperRegistry;
import org.apache.zookeeper.KeeperException;
import org.junit.*;

import java.util.Arrays;
import java.util.List;

public class LoadBalancerEnd2EndIT {
    private static TestingServer testingServer;
    private static CuratorFramework curatorFramework;
    private static final Log LOG = LogFactory.getLog(LoadBalancerEnd2EndIT.class);
    private static final LoadBalanceZookeeperConf LOAD_BALANCER_CONFIGURATION = new LoadBalanceZookeeperConfImpl();
    private static  String path;
    private static LoadBalancer loadBalancer;
    private static HostAndPort pqs1 = HostAndPort.fromParts("localhost",1000);
    private static HostAndPort pqs2 = HostAndPort.fromParts("localhost",2000);
    private static HostAndPort pqs3 = HostAndPort.fromParts("localhost",3000);
    public static String zkConnectString;
    public static Registry registry;

    @BeforeClass
    public  static void setup() throws Exception{

        registry = new ZookeeperRegistry();
        zkConnectString = LOAD_BALANCER_CONFIGURATION.getZkConnectString();
        int port = Integer.parseInt(zkConnectString.split(":")[1]);
        testingServer = new TestingServer(port);
        testingServer.start();

        path = LOAD_BALANCER_CONFIGURATION.getParentPath();
        curatorFramework = CuratorFrameworkFactory.newClient(zkConnectString,
                new ExponentialBackoffRetry(1000, 3));
        curatorFramework.start();
        createNodeForTesting(Arrays.asList(pqs1,pqs2,pqs3));
        curatorFramework.setACL().withACL(LOAD_BALANCER_CONFIGURATION.getAcls());
        loadBalancer = LoadBalancer.getLoadBalancer();
    }

    @AfterClass
    public  static void tearDown() throws Exception {
        CloseableUtils.closeQuietly(curatorFramework);
        CloseableUtils.closeQuietly(testingServer);
    }

    private  static void createNodeForTesting(List<HostAndPort> pqsNodes) throws Exception{
        for(HostAndPort pqs:pqsNodes) {
            registry.registerServer(LOAD_BALANCER_CONFIGURATION,pqs.getPort(),zkConnectString,pqs.getHostText());
        }
        curatorFramework.getChildren().forPath(LOAD_BALANCER_CONFIGURATION.getParentPath()).size();
    }


    @Test
    public void testGetAllServiceLocation() throws Exception {
        Assert.assertNotNull(loadBalancer);
        List<HostAndPort> serviceLocations = loadBalancer.getAllServiceLocation();
        Assert.assertTrue(" must contains 3 service location",serviceLocations.size() == 3);
    }

    @Test
    public void testGetSingleServiceLocation() throws Exception {
        Assert.assertNotNull(loadBalancer);
        HostAndPort serviceLocation = loadBalancer.getSingleServiceLocation();
        Assert.assertNotNull(serviceLocation);
    }

    @Test(expected=Exception.class)
    public void testZookeeperDown() throws Exception{
       testingServer.stop();
       CuratorZookeeperClient zookeeperClient =  curatorFramework.getZookeeperClient();
        //check to see if zookeeper is really down.
       while (zookeeperClient.isConnected()){
            Thread.sleep(1000);
       };
       loadBalancer.getSingleServiceLocation();
    }

    @Test(expected = KeeperException.NoNodeException.class)
    public void testNoPhoenixQueryServerNodeInZookeeper() throws Exception{
        List<HostAndPort> hostAndPorts = Arrays.asList(pqs1, pqs2, pqs3);
        for(HostAndPort pqs: hostAndPorts) {
            String fullPathToNode = LOAD_BALANCER_CONFIGURATION.getFullPathToNode(pqs);
            curatorFramework.delete().deletingChildrenIfNeeded().forPath(fullPathToNode);
            while (curatorFramework.checkExists().forPath(fullPathToNode) != null){
                //wait for the node to deleted
                Thread.sleep(1000);
            };
        }
        //delete the parent
        curatorFramework.delete().forPath(path);
        // should throw an exception as there is
        // no node in the zookeeper
        try {
            loadBalancer.getSingleServiceLocation();
        } catch(Exception e) {
            throw e;
        } finally {
            // need to create node for other tests to run.
            createNodeForTesting(hostAndPorts);
        }
    }

    @Test
    public void testSingletonPropertyForLoadBalancer(){
        LoadBalancer anotherloadBalancerRef = LoadBalancer.getLoadBalancer();
        Assert.assertTrue(" the load balancer is not singleton",loadBalancer == anotherloadBalancerRef );
    }



}
