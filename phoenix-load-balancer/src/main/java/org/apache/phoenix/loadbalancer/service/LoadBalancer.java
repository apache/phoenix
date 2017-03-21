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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.phoenix.loadbalancer.exception.NoPhoenixQueryServerRegisteredException;

import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.io.Closeable;
import java.util.List;


public class LoadBalancer {

    private static final Config CONFIG = new Config();
    private static CuratorFramework curaFramework = null;
    protected static final Log LOG = LogFactory.getLog(LoadBalancer.class);
    private static PathChildrenCache   cache = null;
    private static final LoadBalancer loadBalancer = new LoadBalancer();

    private LoadBalancer()  {
        ConnectionStateListener connectionStateListener = null;
        UnhandledErrorListener unhandledErrorListener = null;
        List<Closeable> closeAbles = Lists.newArrayList();
        try  {

            curaFramework = CuratorFrameworkFactory.newClient(getZkConnectString(),
                    new ExponentialBackoffRetry(1000, 3));
            curaFramework.start();
            connectionStateListener = getConnectionStateListener();
            curaFramework.getConnectionStateListenable()
                    .addListener(connectionStateListener);
            unhandledErrorListener = getUnhandledErrorListener();
            curaFramework.getUnhandledErrorListenable()
                    .addListener(unhandledErrorListener);
            cache = new PathChildrenCache(curaFramework, getBasePath(), true);
            cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            closeAbles.add(cache);
            closeAbles.add(curaFramework);
        }catch(Exception ex){
            LOG.error("Exception while creating a zookeeper clients and cache",ex);
            if ((curaFramework != null) && (connectionStateListener != null)){
                curaFramework.getConnectionStateListenable()
                        .removeListener(connectionStateListener);
            }
            if ((curaFramework != null) && (unhandledErrorListener != null)){
                curaFramework.getUnhandledErrorListenable()
                        .removeListener(unhandledErrorListener);
            }
            for (Closeable closeable : closeAbles) {
                CloseableUtils.closeQuietly(closeable);
            }
        }
    }

    /**
     * Return Singleton Load Balancer every single time.
     * @return LoadBalancer
     */
    public static LoadBalancer getLoadBalancer() {
        return loadBalancer;
    }

    /**
     * return the location of Phoenix Query Server
     * in form of String. The method return a random PQS server
     * from the cache.
     * @return - PQS Location ( host:port)
     * @throws Exception
     */
    public String getSingleServiceLocation()  throws Exception{
        List<ChildData> childNodes = conductSanityCheckAndReturn();
        // get an random connect string
        int i = ThreadLocalRandom.current().nextInt(0, childNodes.size());
        ChildData childData = childNodes.get(i);
        return new String(childData.getData(),"UTF-8");
    }

    /**
     * return all the location of Phoenix Query Server from cache
     * @return - PQS Locations ( host1:port1,host2:port2)
     * @throws Exception
     */
    public List<String> getAllServiceLocation()  throws Exception{
        List<ChildData> childNodes = conductSanityCheckAndReturn();
        // get an random connect string
        List<String> nodesInString = new ArrayList<>();
        for(ChildData node:childNodes){
            nodesInString.add(node.toString());
        }
        return nodesInString;
    }

    private List<ChildData> conductSanityCheckAndReturn() throws Exception{
        Preconditions.checkNotNull(curaFramework
                ," curator framework in not initialized ");
        Preconditions.checkNotNull(cache," cache value is not initialized");
        boolean connected = curaFramework.getZookeeperClient().isConnected();
        if (!connected) {
            String message = " Zookeeper seems to be down. The data is stale ";
            Exception exception =
                    new Exception(message);
            LOG.error(message, exception);
            throw exception;
        }
        List<ChildData> currentData = cache.getCurrentData();
        if (currentData.size() == 0) {
            LOG.info(" No Phoenix query server instance found in zookeeper");
            throw new NoPhoenixQueryServerRegisteredException();
        }
        return currentData;
    }
    private String getZkConnectString(){
        return CONFIG.getZkConnectString();
    }

    private String getBasePath(){
        return CONFIG.getQueryServerBasePath();
    }

    private ConnectionStateListener getConnectionStateListener(){
        return new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                if (!newState.isConnected()) {
                    LOG.error(" connection to zookeeper broken ");
                }
            }
        };
    }

    private UnhandledErrorListener getUnhandledErrorListener(){
        return new UnhandledErrorListener() {
            @Override
            public void unhandledError(String message, Throwable e) {
                LOG.error("unhandled exception ",e);
            }
        };
    }

}
