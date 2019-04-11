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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.exception.PhoenixNonRetryableRuntimeException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class GuidePostsCacheProviderTest {

    static GuidePostsCache testCache = null;

    public static class TestGuidePostsCacheFactory implements  GuidePostsCacheFactory {

        public static volatile int count=0;

        public TestGuidePostsCacheFactory() {
            count++;
        }

        @Override
        public GuidePostsCache getGuidePostsCacheInterface(ConnectionQueryServices queryServices,
                Configuration config) {
            return testCache;
        }
    }



    public static CountDownLatch latch1 ;
    public static CountDownLatch latchCollect ;
    ConcurrentMap<GuidePostsCacheFactory,Object> concurrentMap;


    public static class TestGuidePostsLatchedCacheFactory implements  GuidePostsCacheFactory {
        public TestGuidePostsLatchedCacheFactory() {
            try {
                latch1.await();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public GuidePostsCache getGuidePostsCacheInterface(ConnectionQueryServices queryServices,
                Configuration config) {
            return testCache;
        }
    }

    public static class TestGuidePostsCacheFactoryNoConstructor implements  GuidePostsCacheFactory {

        private TestGuidePostsCacheFactoryNoConstructor(){}

        @Override
        public GuidePostsCache getGuidePostsCacheInterface(ConnectionQueryServices queryServices,
                Configuration config) {
            return null;
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

    @Test(expected = PhoenixNonRetryableRuntimeException.class)
    public void loadAndGetGuidePostsCacheFactoryNoDefaultConstructorClassFailure(){
        GuidePostsCacheFactory factory = helper.loadAndGetGuidePostsCacheFactory(
                TestGuidePostsCacheFactoryNoConstructor.class.getTypeName());
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

    private Runnable getRunnable(){
        return new Runnable() {
            @Override public void run() {
                try {
                    GuidePostsCacheFactory
                            factory =
                            helper.loadAndGetGuidePostsCacheFactory(
                                    TestGuidePostsLatchedCacheFactory.class.getTypeName());
                    concurrentMap.putIfAbsent(factory, new Object());
                    latchCollect.countDown();
                } catch (Exception e){
                    throw new RuntimeException(e);
                }
            }
        };
    }


    @Test
    public void getSingletonMultiThreadedTest() {
        latch1 = new CountDownLatch(1);
        latchCollect = new CountDownLatch(2);
        concurrentMap = new ConcurrentHashMap<>();

        ExecutorService executorService = null;
        try {
            executorService = Executors.newFixedThreadPool(2);
            executorService.submit(getRunnable());
            executorService.submit(getRunnable());

            assertEquals(0, concurrentMap.size());

            // unblock the constructor, should only be in one thread
            Thread.sleep(100);
            latch1.countDown();

            // wait for both threads finish calling the method
            try {
                latchCollect.await();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            // Map has 1 entry
            assertEquals(1, concurrentMap.size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            executorService.shutdown();
        }
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
