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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.phoenix.loadbalancer.exception.NoPhoenixQueryServerRegisteredException;
import org.apache.phoenix.loadbalancer.service.LoadBalancer;
import org.apache.phoenix.loadbalancer.service.LoadBalancerConfiguration;
import org.apache.phoenix.loadbalancer.service.PhoenixQueryServerNode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.*;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class LoadBalancerEnd2End  {
    private static TestingServer testingServer;
    private static CuratorFramework curatorFramework;
    private static final Log LOG = LogFactory.getLog(LoadBalancerEnd2End.class);
    private static final LoadBalancerConfiguration LOAD_BALANCER_CONFIGURATION = new LoadBalancerConfiguration();
    private static  String path;
    private static LoadBalancer loadBalancer;
    private static PhoenixQueryServerNode pqs1 = new PhoenixQueryServerNode("localhost","1000");
    private static PhoenixQueryServerNode pqs2 = new PhoenixQueryServerNode("localhost","2000");
    private static PhoenixQueryServerNode pqs3 = new PhoenixQueryServerNode("localhost","3000");

    @BeforeClass
    public  static void setup() throws Exception{
        String zkConnectString = LOAD_BALANCER_CONFIGURATION.getZkConnectString();
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

    private  static void createNodeForTesting(List<PhoenixQueryServerNode> pqsNodes) throws Exception{
        for(PhoenixQueryServerNode pqs:pqsNodes) {
            curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(
                    LOAD_BALANCER_CONFIGURATION.getFullPathToNode(pqs.getHost(),pqs.getPort())
                    , pqs.toJsonString().getBytes(StandardCharsets.UTF_8));
        }
        curatorFramework.getChildren().forPath(LOAD_BALANCER_CONFIGURATION.getParentPath()).size();
    }


    @Test
    public void testGetAllServiceLocation() throws Exception {
        Assert.assertNotNull(loadBalancer);
        List<PhoenixQueryServerNode> serviceLocations = loadBalancer.getAllServiceLocation();
        Assert.assertTrue(" must contains 3 service location",serviceLocations.size() == 3);
    }

    @Test
    public void testGetSingleServiceLocation() throws Exception {
        Assert.assertNotNull(loadBalancer);
        PhoenixQueryServerNode serviceLocation = loadBalancer.getSingleServiceLocation();
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
        curatorFramework.delete().deletingChildrenIfNeeded().forPath(LOAD_BALANCER_CONFIGURATION.getParentPath());
        while (curatorFramework.checkExists().forPath(path) != null){
            //wait for the node to deleted
            Thread.sleep(1000);
        };
        // should throw an exception as there is
        // no node in the zookeeper
        try {
            loadBalancer.getSingleServiceLocation();
        } catch(Exception e) {
            throw e;
        } finally {
            // need to create node for other tests to run.
            createNodeForTesting(Arrays.asList(pqs1,pqs2,pqs3));
        }
    }

    @Test
    public void testSingletonPropertyForLoadBalancer(){
        LoadBalancer anotherloadBalancerRef = LoadBalancer.getLoadBalancer();
        Assert.assertTrue(" the load balancer is not singleton",loadBalancer == anotherloadBalancerRef );
    }



}
