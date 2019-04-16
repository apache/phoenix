/**
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.exception.PhoenixNonRetryableRuntimeException;
import org.apache.phoenix.util.InstanceResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.phoenix.query.QueryServices.STATS_COLLECTION_ENABLED;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_STATS_COLLECTION_ENABLED;

public class GuidePostsCacheProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(GuidePostsCacheProvider.class);

    GuidePostsCacheFactory guidePostsCacheFactory = null;

    @VisibleForTesting
    GuidePostsCacheFactory loadAndGetGuidePostsCacheFactory(String classString) {
        Preconditions.checkNotNull(classString);
        if (guidePostsCacheFactory == null) {
            try {

                Class clazz = ClassLoader.getSystemClassLoader().loadClass(classString);
                if(!GuidePostsCacheFactory.class.isAssignableFrom(clazz)){
                    String msg = String.format("Could not load/instantiate class %s is not an instance of GuidePostsCacheFactory", classString);
                    LOGGER.error(msg);
                    throw new PhoenixNonRetryableRuntimeException(msg);
                }

                List<GuidePostsCacheFactory>
                        factoryList = InstanceResolver.get(GuidePostsCacheFactory.class,null);
                for(GuidePostsCacheFactory factory : factoryList){
                    if (clazz.isInstance(factory)) {
                        guidePostsCacheFactory = factory;
                        LOGGER.info(String.format("Sucessfully loaded class for GuidePostsCacheFactor of type: %s",classString));
                    }
                }
                if(guidePostsCacheFactory == null){
                    String msg = String.format("Could not load/instantiate class %s", classString);
                    LOGGER.error(msg);
                    throw new PhoenixNonRetryableRuntimeException(msg);
                }
            } catch (ClassNotFoundException e) {
                LOGGER.error(String.format("Could not load/instantiate class %s", classString), e);
                throw new PhoenixNonRetryableRuntimeException(e);
            }
        }
        return guidePostsCacheFactory;
    }

    public GuidePostsCacheWrapper getGuidePostsCache(String classStr, ConnectionQueryServices queryServices, Configuration config) {
        final boolean isStatsEnabled = config.getBoolean(STATS_COLLECTION_ENABLED, DEFAULT_STATS_COLLECTION_ENABLED);
        PhoenixStatsLoader phoenixStatsLoader = isStatsEnabled ? new StatsLoaderImpl(queryServices) : new EmptyStatsLoader();

        GuidePostsCacheFactory
                guidePostCacheFactory = loadAndGetGuidePostsCacheFactory(classStr);
        GuidePostsCache
                guidePostsCache =
                guidePostCacheFactory.getGuidePostsCache(phoenixStatsLoader, config);
        return new GuidePostsCacheWrapper(guidePostsCache);
    }
}
