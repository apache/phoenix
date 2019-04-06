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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryServicesHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryServicesHelper.class);

    // Singleton protected by synchronized method
    static GuidePostsCacheFactory guidePostsCacheFactory = null;

    @VisibleForTesting
    synchronized GuidePostsCacheFactory loadAndGetGuidePostsCacheFactory(String classString) {
        Preconditions.checkNotNull(classString);
        if (guidePostsCacheFactory == null) {
            try {
                Class clazz = ClassLoader.getSystemClassLoader().loadClass(classString);
                Object o = clazz.newInstance();
                if (!(o instanceof GuidePostsCacheFactory)) {
                    String msg = String.format("Class %s not an instance of GuidePostsCacheFactory", classString);
                    LOGGER.error(msg);
                    throw new PhoenixNonRetryableRuntimeException(msg);
                }
                guidePostsCacheFactory = (GuidePostsCacheFactory)o;
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                LOGGER.error(String.format("Could not load/instantiate class %s", classString), e);
                throw new PhoenixNonRetryableRuntimeException(e);
            }
        }
        return guidePostsCacheFactory;
    }

    public GuidePostsCache getGuidePostsCache(String classStr, ConnectionQueryServices queryServices, Configuration config) {
        GuidePostsCacheFactory
                guidePostCacheFactory = loadAndGetGuidePostsCacheFactory(classStr);
        GuidePostsCache
                guidePostsCache =
                guidePostCacheFactory.getGuidePostsCacheInterface(queryServices, config);
        return guidePostsCache;
    }

    @VisibleForTesting
    synchronized static void initialize() {
        guidePostsCacheFactory = null;
    }
}
