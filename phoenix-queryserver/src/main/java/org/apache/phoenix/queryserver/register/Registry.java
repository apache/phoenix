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


import org.apache.phoenix.loadbalancer.service.LoadBalanceZookeeperConf;

/**
 * Registry interface for implementing registering
 * and un-registering to service locator.
 */
public interface  Registry  {

    /**
     * Unreqister the server with zookeeper. All Cleanup
     * is done in this method.
     * @throws Exception
     */
    void unRegisterServer() throws Exception;

    /**
     * Registers the server with the service locator ( zookeeper).
     * @param configuration - Hbase Configuration
     * @param port - port for PQS server
     * @param connectString - zookeeper connect string
     * @param pqsHost - host for PQS server.
     * @throws Exception
     */
    void registerServer(LoadBalanceZookeeperConf configuration, int port
            , String connectString, String pqsHost) throws Exception ;

}
