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
package org.apache.phoenix.metrics;

import java.util.Arrays;

import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

import com.google.common.base.Preconditions;

/**
 *
 */
public class MetricsManagerImpl implements MetricsManager {

  private MetricsSystem system;

  @Override
  /**
   * Register a metrics sink
   * @param <T>   the type of the sink.
   * @param sink  to register
   * @param name  of the sink. Must be unique.
   * @param desc  the description of the sink
   * @return the sink
   * @throws IllegalArgumentException if sink is not a MetricsSink
   */
  public <T> T register(String name, String desc, T sink) {
    isA(sink, MetricsSink.class);
    return (T) system.register(name, desc, (MetricsSink) sink);
  }

  public <T> T registerSource(String name, String desc, T source) {
    isA(source, MetricsSource.class);
    return (T) system.register(name, desc, (MetricsSource) source);
  }

  @Override
  public void initialize(String prefix) {
    this.system = DefaultMetricsSystem.initialize(prefix);
  }

  private <T> void isA(T object, Class<?>... classes) {
    boolean match = false;
    for (Class<?> clazz : classes) {
      if (clazz.isAssignableFrom(object.getClass())) {
        match = true;
        break;
      }
    }
    Preconditions.checkArgument(match, object + " is not one of " + Arrays.toString(classes));
  }

  @Override
  public void shutdown() {
    if (this.system != null) {
      this.system.shutdown();
    }
  }
}
