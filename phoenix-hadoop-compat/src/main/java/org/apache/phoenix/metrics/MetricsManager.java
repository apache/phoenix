/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.metrics;

/**
 * Metrics management system. Backed by the underlying hadoop metrics, depending on the project on
 * the classpath.
 * <p>
 * The underlying types passed to method must match the expected metrics type - this will vary for
 * the underlying metrics systems (hadoop1 vs hadoop2), but can't be specified at this layer because
 * we must be compatible with both systems.
 */
public interface MetricsManager {

    /**
     * @param metricsSystemName the metrics prefix to initialize, if it hasn't already been
     *            initialized. Not assumed to be thread-safe, unless otherwise noted in the
     *            implementation.
     */
    public abstract void initialize(String metricsSystemName);

    /**
     * Register a metrics sink
     * @param <T> the type of the sink
     * @param sink to register
     * @param name of the sink. Must be unique.
     * @param desc the description of the sink
     * @return the sink
     */
    public abstract <T> T register(String name, String desc, T sink);

    /**
     * Register a metrics source.
     * @param name name of the source - must be unique
     * @param description description of the source
     * @param source to register.
     * @param <T> the type of the source
     * @return the source
     */
    public abstract <T> T registerSource(String name, String description, T source);

    public void shutdown();
}