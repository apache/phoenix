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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.exception.PhoenixNonRetryableRuntimeException;
import org.apache.phoenix.util.InstanceResolver;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class GuidePostsCacheProviderTest {

    static GuidePostsCache testCache = null;
    static PhoenixStatsLoader phoenixStatsLoader = null;

    public static class TestGuidePostsCacheFactory implements  GuidePostsCacheFactory {

        public static volatile int count=0;

        public TestGuidePostsCacheFactory() {
            count++;
        }

        @Override public PhoenixStatsLoader getPhoenixStatsLoader(
                ConnectionQueryServices clientConnectionQueryServices, ReadOnlyProps readOnlyProps,
                Configuration config) {
            return phoenixStatsLoader;
        }

        @Override
        public GuidePostsCache getGuidePostsCache(PhoenixStatsLoader phoenixStatsLoader,
                Configuration config) {
            return testCache;
        }
    }

    private GuidePostsCacheProvider helper;

    @Before public void init(){
        TestGuidePostsCacheFactory.count = 0;
        helper = new GuidePostsCacheProvider();
    }


    @Test(expected = java.lang.NullPointerException.class)
    public void loadAndGetGuidePostsCacheFactoryNullStringFailure(){
            helper.loadAndGetGuidePostsCacheFactory(null);
    }

    @Test(expected = PhoenixNonRetryableRuntimeException.class)
    public void loadAndGetGuidePostsCacheFactoryBadStringFailure(){
        helper.loadAndGetGuidePostsCacheFactory("not a class");
    }

    @Test(expected = PhoenixNonRetryableRuntimeException.class)
    public void loadAndGetGuidePostsCacheFactoryNonImplementingClassFailure(){
        helper.loadAndGetGuidePostsCacheFactory(Object.class.getTypeName());
    }

    @Test
    public void loadAndGetGuidePostsCacheFactoryTestFactory(){
        GuidePostsCacheFactory factory = helper.loadAndGetGuidePostsCacheFactory(
                TestGuidePostsCacheFactory.class.getTypeName());
        assertTrue(factory instanceof TestGuidePostsCacheFactory);
    }


    @Test
    public void getSingletonSimpleTest(){
        GuidePostsCacheFactory factory1 = helper.loadAndGetGuidePostsCacheFactory(
                TestGuidePostsCacheFactory.class.getTypeName());
        assertTrue(factory1 instanceof TestGuidePostsCacheFactory);

        GuidePostsCacheFactory factory2 = helper.loadAndGetGuidePostsCacheFactory(
                TestGuidePostsCacheFactory.class.getTypeName());
        assertTrue(factory2 instanceof TestGuidePostsCacheFactory);

        assertEquals(factory1,factory2);
        assertEquals(1,TestGuidePostsCacheFactory.count);
    }

    @Test
    public void getGuidePostsCacheWrapper(){
        testCache = Mockito.mock(GuidePostsCache.class);
        ConnectionQueryServices mockQueryServices = Mockito.mock(ConnectionQueryServices.class);
        Configuration mockConfiguration = Mockito.mock(Configuration.class);
        GuidePostsCacheWrapper
                value =
                helper.getGuidePostsCache(TestGuidePostsCacheFactory.class.getTypeName(),
                        mockQueryServices, mockConfiguration);
        value.invalidateAll();
        Mockito.verify(testCache,Mockito.atLeastOnce()).invalidateAll();
    }
}
