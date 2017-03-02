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
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Created by rshrivastava on 3/1/17.
 */
public class GuiceModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(String.class).annotatedWith(Names.named("BASE_PATH"))
                    .toInstance("/services");
            bind(String.class).annotatedWith(Names.named("serviceName"))
                    .toInstance("phoenix_query_servers");
            bind(String.class).annotatedWith(Names.named("zookeeperConnectionString"))
                    .toInstance("localhost:2181");
            bind(Optional.class).toProvider(ZookeeperServiceDiscovererProvider.class).in(Singleton.class);
        }

        @Provides
        @Singleton
        Configuration getConf() {
            return HBaseConfiguration.create();
        }




}
