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
package org.apache.phoenix.loadbalancer.inject;


import com.google.common.base.Optional;
import com.google.inject.Provider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.phoenix.loadbalancer.service.ZookeeperServiceDiscoverer;


public class ZookeeperServiceDiscovererProvider implements Provider<Optional<ZookeeperServiceDiscoverer>> {

    @Override
    public Optional<ZookeeperServiceDiscoverer> get() {
        ZookeeperServiceDiscoverer zookeeperServiceDiscoverer=null;
        try {
            zookeeperServiceDiscoverer = new ZookeeperServiceDiscoverer("localhost:2181",
                    new ExponentialBackoffRetry(1000, 1));
        }catch(Exception ex) {

        } finally {
            return Optional.of(zookeeperServiceDiscoverer);
        }

    }
}

