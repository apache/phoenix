/*
 *
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
package org.apache.phoenix.util;

/**
 * Manages a singleton instance of the environment edge. This class shall
 * implement static versions of the interface {@link EnvironmentEdge}, then
 * defer to the delegate on invocation.
 */
public class EnvironmentEdgeManager {
  private static volatile EnvironmentEdge delegate = new DefaultEnvironmentEdge();

  private EnvironmentEdgeManager() {

  }

  /**
   * Retrieves the singleton instance of the {@link EnvironmentEdge} that is
   * being managed.
   *
   * @return the edge.
   */
  public static EnvironmentEdge getDelegate() {
    return delegate;
  }

  /**
   * Resets the managed instance to the default instance: {@link
   * DefaultEnvironmentEdge}.
   */
  public static void reset() {
    injectEdge(new DefaultEnvironmentEdge());
  }

  /**
   * Injects the given edge such that it becomes the managed entity. If null is
   * passed to this method, the default type is assigned to the delegate.
   *
   * <b>Note: This is JVM global. Make sure to call reset() after the test.</b>
   * See org.apache.hadoop.hbase.util.EnvironmentEdgeManager for other caveats
   *
   * @param edge the new edge.
   */
  public static void injectEdge(EnvironmentEdge edge) {
    org.apache.hadoop.hbase.util.EnvironmentEdgeManager.injectEdge(edge);
    if (edge == null) {
      reset();
    } else {
      delegate = edge;
    }
  }

  /**
   * Defers to the delegate and calls the
   * {@link EnvironmentEdge#currentTime()} method.
   *
   * @return current time in millis according to the delegate.
   */
  public static long currentTimeMillis() {
    return getDelegate().currentTime();
  }
}
