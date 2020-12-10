/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.util.ReadOnlyProps;

/**
 * Interface for configurable GuidePostsCache interface construction
 * Class is meant to be defined in the ConnectionQueryServices property
 * Implementations must provide a default constructor
 */
public interface GuidePostsCacheFactory {

    /**
     * Interface for a PhoenixStatsLoader
     * @param clientConnectionQueryServices current client connectionQueryServices note not
     *                                      necessary to use this connection
     * @param readOnlyProps properties from HBase configuration
     * @param config a Configuration for the current Phoenix/Hbase
     * @return PhoenixStatsLoader interface
     */
    PhoenixStatsLoader getPhoenixStatsLoader(ConnectionQueryServices clientConnectionQueryServices, ReadOnlyProps readOnlyProps, Configuration config);

    /**
     * @param phoenixStatsLoader The passed in stats loader will come from getPhoenixStatsLoader
     * @param config a Configuration for the current Phoenix/Hbase
     * @return GuidePostsCache interface
     */
    GuidePostsCache getGuidePostsCache(PhoenixStatsLoader phoenixStatsLoader, Configuration config);
}
