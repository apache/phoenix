/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

public class NoOpCache implements Cache {

    public NoOpCache() {
    }

    @Nullable
    @Override
    public Object getIfPresent(Object key) {
        return null;
    }

    @Override
    public Object get(Object key, Callable valueLoader) throws ExecutionException {
        return null;
    }

    @Override
    public void put(Object key, Object value) {

    }

    @Override
    public void putAll(Map m) {

    }

    @Override
    public void invalidate(Object key) {

    }

    @Override
    public void invalidateAll() {

    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public CacheStats stats() {
        return null;
    }

    @Override
    public ConcurrentMap asMap() {
        return null;
    }

    @Override
    public void cleanUp() {

    }

    @Override
    public void invalidateAll(Iterable keys) {

    }

    @Override
    public ImmutableMap getAllPresent(Iterable keys) {
        return null;
    }
}
