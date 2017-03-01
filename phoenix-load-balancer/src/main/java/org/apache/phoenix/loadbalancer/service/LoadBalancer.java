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
import com.google.common.net.HostAndPort;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.io.Closeable;
import java.util.List;

/**
 * LoadBalancer class is singleton , used by the client
 * to find out about various location of PQS servers.
 * The client needs to configure the HBase-site.xml with the needed
 * properties i.e. location of zookeeper ( or service locator), username, password etc.
 */
public class LoadBalancer {

    private static final LoadBalanceZookeeperConf CONFIG = new LoadBalanceZookeeperConfImpl(HBaseConfiguration.create());
    private static CuratorFramework curaFramework = null;
    protected static final Log LOG = LogFactory.getLog(LoadBalancer.class);
    private static PathChildrenCache   cache = null;
    private static final LoadBalancer loadBalancer = new LoadBalancer();
    private ConnectionStateListener connectionStateListener = null;
    private UnhandledErrorListener unhandledErrorListener = null;
    private List<Closeable> closeAbles = Lists.newArrayList();

    private LoadBalancer()  {
        try  {
            start();
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
     * It returns the location of Phoenix Query Server
     * in form of Guava <a href="https://google.github.io/guava/releases/19.0/api/docs/com/google/common/net/HostAndPort.html">HostAndPort</a>
     * from the cache. The client should catch Exception incase
     * the method is unable to fetch PQS location due to network failure or
     * in-correct configuration issues.
     * @return - return Guava HostAndPort. See <a href="http://google.com">http://google.com</a>
     * @throws Exception
     */
    public HostAndPort getSingleServiceLocation()  throws Exception{
        List<HostAndPort> childNodes = conductSanityCheckAndReturn();
        // get an random connect string
        int i = ThreadLocalRandom.current().nextInt(0, childNodes.size());
        return childNodes.get(i);
    }

    /**
     * return locations of all Phoenix Query Servers
     * in the form of a List of PQS servers  <a href="https://google.github.io/guava/releases/19.0/api/docs/com/google/common/net/HostAndPort.html">HostAndPort</a>
     * @return - HostAndPort
     * @throws Exception
     */
    public List<HostAndPort> getAllServiceLocation()  throws Exception{
        return conductSanityCheckAndReturn();
    }

    private List<HostAndPort> conductSanityCheckAndReturn() throws Exception{
        Preconditions.checkNotNull(curaFramework
                ," curator framework in not initialized ");
        Preconditions.checkNotNull(cache," cache value is not initialized");
        boolean connected = curaFramework.getZookeeperClient().isConnected();
        if (!connected) {
            String message = " Zookeeper seems to be down. The data is stale ";
            ConnectException exception =
                    new ConnectException(message);
            LOG.error(message, exception);
            throw exception;
        }
        List<String> currentNodes = curaFramework.getChildren().forPath(CONFIG.getParentPath());
        List<HostAndPort> returnNodes = new ArrayList<>();
        String nodeAsString = null;
        for(String node:currentNodes) {
            try {
                returnNodes.add(HostAndPort.fromString(node));
            } catch(Throwable ex) {
                LOG.error(" something wrong with node string "+nodeAsString,ex);
            }
        }
        return returnNodes;
    }
    private String getZkConnectString(){
        return CONFIG.getZkConnectString();
    }

    private ConnectionStateListener getConnectionStateListener(){
        return new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                if (!newState.isConnected()) {
                    LOG.error( " connection to zookeeper broken. It is in  "+ newState.name()+" state.");
                }
            }
        };
    }

    private UnhandledErrorListener getUnhandledErrorListener(){
        return new UnhandledErrorListener() {
            @Override
            public void unhandledError(String message, Throwable e) {
                LOG.error("unhandled exception:  "+ message,e);
            }
        };
    }

    private void start() throws Exception{
        curaFramework = CuratorFrameworkFactory.newClient(getZkConnectString(),
                new ExponentialBackoffRetry(1000, 3));
        curaFramework.start();
        curaFramework.setACL().withACL(CONFIG.getAcls());
        connectionStateListener = getConnectionStateListener();
        curaFramework.getConnectionStateListenable()
                .addListener(connectionStateListener);
        unhandledErrorListener = getUnhandledErrorListener();
        curaFramework.getUnhandledErrorListenable()
                .addListener(unhandledErrorListener);
        cache = new PathChildrenCache(curaFramework, CONFIG.getParentPath(), true);
        cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        closeAbles.add(cache);
        closeAbles.add(curaFramework);
    }
}
