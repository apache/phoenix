/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.query;

import static org.apache.phoenix.query.QueryServices.STATS_COLLECTION_ENABLED;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_STATS_COLLECTION_ENABLED;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.util.ReadOnlyProps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

public class DefaultGuidePostsCacheFactory implements GuidePostsCacheFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultGuidePostsCacheFactory.class);

    @Override
    public PhoenixStatsLoader getPhoenixStatsLoader(ConnectionQueryServices queryServices, ReadOnlyProps readOnlyProps,
            Configuration config) {
        Preconditions.checkNotNull(config);

        final boolean isStatsEnabled = config.getBoolean(STATS_COLLECTION_ENABLED, DEFAULT_STATS_COLLECTION_ENABLED);
        if (queryServices == null || !isStatsEnabled) {
            LOGGER.info("Using EmptyStatsLoader from DefaultGuidePostsCacheFactory");
            return new EmptyStatsLoader();
        }
        return new StatsLoaderImpl(queryServices);
    }

    @Override
    public GuidePostsCache getGuidePostsCache(PhoenixStatsLoader phoenixStatsLoader, Configuration config) {
        LOGGER.debug("DefaultGuidePostsCacheFactory guide post cache construction.");
        PhoenixStatsCacheLoader cacheLoader = new PhoenixStatsCacheLoader(phoenixStatsLoader, config);
        return new GuidePostsCacheImpl(cacheLoader, config);
    }
}
