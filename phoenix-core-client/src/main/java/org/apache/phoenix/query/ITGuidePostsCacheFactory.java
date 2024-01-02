/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.query;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.util.ReadOnlyProps;

/**
 * Test Class Only used to verify in e2e tests
 */
public class ITGuidePostsCacheFactory implements GuidePostsCacheFactory {
    public static final ConcurrentHashMap<Integer, DefaultGuidePostsCacheFactory> map =
            new ConcurrentHashMap<>();
    private static AtomicInteger count = new AtomicInteger();
    private Integer key;

    public ITGuidePostsCacheFactory() {
        key = count.getAndIncrement();
        map.put(key, new DefaultGuidePostsCacheFactory());
    }

    public static int getCount() {
        return count.get();
    }

    public static ConcurrentHashMap<Integer, DefaultGuidePostsCacheFactory> getMap(){
        return map;
    }

    @Override
    public PhoenixStatsLoader getPhoenixStatsLoader(ConnectionQueryServices queryServices,
            ReadOnlyProps readOnlyProps, Configuration config) {
        return map.get(key).getPhoenixStatsLoader(queryServices, readOnlyProps, config);
    }

    @Override
    public GuidePostsCache getGuidePostsCache(PhoenixStatsLoader phoenixStatsLoader,
            Configuration config) {
        return map.get(key).getGuidePostsCache(phoenixStatsLoader, config);
    }
}
