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

package org.apache.phoenix.coprocessorclient.metrics;
/**
 * Factory object to create various metric sources for phoenix related coprocessors.
 */
public class MetricsPhoenixCoprocessorSourceFactory {

    private static final MetricsPhoenixCoprocessorSourceFactory
            INSTANCE = new MetricsPhoenixCoprocessorSourceFactory();
    // Holds the PHOENIX_TTL related metrics.
    private static volatile MetricsPhoenixTTLSource phoenixTTLSource;

    public static MetricsPhoenixCoprocessorSourceFactory getInstance() {
        return INSTANCE;
    }

    // return the metric source for PHOENIX_TTL coproc.
    public MetricsPhoenixTTLSource getPhoenixTTLSource() {
        if (INSTANCE.phoenixTTLSource == null) {
            synchronized (MetricsPhoenixTTLSource.class) {
                if (INSTANCE.phoenixTTLSource == null) {
                    INSTANCE.phoenixTTLSource = new MetricsPhoenixTTLSourceImpl();
                }
            }
        }
        return INSTANCE.phoenixTTLSource;
    }
}
