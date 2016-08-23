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
package org.apache.phoenix.util;

import org.apache.commons.collections.IteratorUtils;

import com.google.common.annotations.VisibleForTesting;

import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves object instances registered using the JDK 6+ {@link java.util.ServiceLoader}.
 *
 * 
 * @since 2.0
 */
public class InstanceResolver {
    private static final ConcurrentHashMap<Class, Object> RESOLVED_SINGLETONS = new ConcurrentHashMap<Class, Object>();

    private InstanceResolver() {/* not allowed */}

    /**
     * Resolves an instance of the specified class if it has not already been resolved.
     * @param clazz The type of instance to resolve
     * @param defaultInstance The instance to use if a custom instance has not been registered
     * @return The resolved instance or the default instance provided.
     *         {@code null} if an instance is not registered and a default is not provided.
     */
    @SuppressWarnings("unchecked")
    public static <T> T getSingleton(Class<T> clazz, T defaultInstance) {
        Object obj = RESOLVED_SINGLETONS.get(clazz);
        if(obj != null) {
            return (T)obj;
        }
        if (defaultInstance != null && !clazz.isInstance(defaultInstance)) throw new IllegalArgumentException("defaultInstance is not of type " + clazz.getName());
        final Object o = resolveSingleton(clazz, defaultInstance);
        obj = RESOLVED_SINGLETONS.putIfAbsent(clazz, o);
        if(obj == null) {
            obj = o;
        }
        return (T)obj;
    }

    /**
     * Resolves all instances of a specified class and add it to the list of default implementations
     * @param clazz Type of the instance to resolve
     * @param defaultInstances {@link List} of instances that match the type clazz
     * @param <T> Type of class passed
     * @return {@link List} of instance of the specified class. Newly found instances will be added
     *          to the existing contents of defaultInstances
     */
    @SuppressWarnings("unchecked")
    public static <T> List get(Class<T> clazz, List<T> defaultInstances) {
        Iterator<T> iterator = ServiceLoader.load(clazz).iterator();
        if (defaultInstances != null) {
            defaultInstances.addAll(IteratorUtils.toList(iterator));
        } else {
            defaultInstances = IteratorUtils.toList(iterator);
        }

        return defaultInstances;
    }

    private synchronized static <T> T resolveSingleton(Class<T> clazz, T defaultInstance) {
        ServiceLoader<T> loader = ServiceLoader.load(clazz);
        // returns the first registered instance found
        for (T singleton : loader) {
            return singleton;
        }
        return defaultInstance;
    }

    @VisibleForTesting
    public static void clearSingletons() {
        RESOLVED_SINGLETONS.clear();
    }
}
