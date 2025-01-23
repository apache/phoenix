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
 * Has some basic interaction with the environment. Alternate implementations
 * can be used where required (eg in tests).
 *
 * @see EnvironmentEdgeManager
 */
public abstract class EnvironmentEdge implements org.apache.hadoop.hbase.util.EnvironmentEdge {
  /**
   * Returns the currentTime.
   *
   * @return Current time.
   */
  @Override
  abstract public long currentTime();

  /**
   * This method can be only be used to measure elapsed time. The method returns a time marker
   * and if we subtract any two time markers returned by this method, then we get time elapsed
   * b/w those two markers in nano seconds. For further details refer to documentation of
   * {@link System#nanoTime()}.
   * @return a time marker in nano seconds
   */
  public long nanoTime() {
    return convertMsToNs(currentTime());
  }

  protected long convertMsToNs(long value) {
    return value * 1000000;
  }

  protected long convertNsToMs(long value) {
    return value / 1000000;
  }
}
