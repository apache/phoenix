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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.exception.PhoenixNonRetryableRuntimeException;
import org.apache.phoenix.util.InstanceResolver;
import org.apache.phoenix.util.ReadOnlyProps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

public class GuidePostsCacheProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(GuidePostsCacheProvider.class);

    GuidePostsCacheFactory guidePostsCacheFactory = null;

    @VisibleForTesting
    GuidePostsCacheFactory loadAndGetGuidePostsCacheFactory(String classString) {
        Preconditions.checkNotNull(classString);
        if (guidePostsCacheFactory == null) {
            try {

                Class clazz = Class.forName(classString);
                if (!GuidePostsCacheFactory.class.isAssignableFrom(clazz)) {
                    String msg = String.format(
                            "Could not load/instantiate class %s is not an instance of GuidePostsCacheFactory",
                            classString);
                    LOGGER.error(msg);
                    throw new PhoenixNonRetryableRuntimeException(msg);
                }

                List<GuidePostsCacheFactory> factoryList = InstanceResolver.get(GuidePostsCacheFactory.class, null);
                for (GuidePostsCacheFactory factory : factoryList) {
                    if (clazz.isInstance(factory)) {
                        guidePostsCacheFactory = factory;
                        LOGGER.info(String.format("Sucessfully loaded class for GuidePostsCacheFactor of type: %s",
                                classString));
                        break;
                    }
                }
                if (guidePostsCacheFactory == null) {
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

    public GuidePostsCacheWrapper getGuidePostsCache(String classStr, ConnectionQueryServices queryServices,
            Configuration config) {
        ReadOnlyProps props = null;
        if (queryServices != null) {
            props = queryServices.getProps();
        }
        GuidePostsCacheFactory guidePostCacheFactory = loadAndGetGuidePostsCacheFactory(classStr);
        PhoenixStatsLoader phoenixStatsLoader = guidePostsCacheFactory.getPhoenixStatsLoader(queryServices, props,
                config);
        GuidePostsCache guidePostsCache = guidePostCacheFactory.getGuidePostsCache(phoenixStatsLoader, config);
        return new GuidePostsCacheWrapper(guidePostsCache);
    }
}
