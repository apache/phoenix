/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.query;

import org.apache.phoenix.util.InstanceResolver;

/**
 * Manages factories that provide extension points for HBase.
 * <p/>
 * Dependent modules may register their own implementations of the following using {@link java.util.ServiceLoader}:
 * <ul>
 *     <li>{@link ConfigurationFactory}</li>
 *     <li>{@link HTableFactory}</li>
 *     <li> {@link HConnectionFactory} </li>
 * </ul>
 *
 * If a custom implementation is not registered, the default implementations will be used.
 *
 * 
 * @since 0.2
 */
public class HBaseFactoryProvider {

    private static final HTableFactory DEFAULT_HTABLE_FACTORY = new HTableFactory.HTableFactoryImpl();
    private static final HConnectionFactory DEFAULT_HCONNECTION_FACTORY =
        new HConnectionFactory.HConnectionFactoryImpl();
    private static final ConfigurationFactory DEFAULT_CONFIGURATION_FACTORY = new ConfigurationFactory.ConfigurationFactoryImpl();

    public static HTableFactory getHTableFactory() {
        return InstanceResolver.getSingleton(HTableFactory.class, DEFAULT_HTABLE_FACTORY);
    }

    public static HConnectionFactory getHConnectionFactory() {
        return InstanceResolver.getSingleton(HConnectionFactory.class, DEFAULT_HCONNECTION_FACTORY);
    }

    public static ConfigurationFactory getConfigurationFactory() {
        return InstanceResolver.getSingleton(ConfigurationFactory.class, DEFAULT_CONFIGURATION_FACTORY);
    }
}
