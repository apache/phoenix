/*
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
package org.apache.phoenix.job;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Delegates to {@link Executors#defaultThreadFactory()}, but keeps working after the ThreadGroup
 * captured by that factory is destroyed.
 * <p/>
 * The default factory places every new thread in the ThreadGroup of the thread that created the
 * factory. Long-lived pools (e.g. owned by the PhoenixDriver singleton) that let idle core threads
 * time out can end up with no live threads in that group. If the group is then destroyed, every
 * later attempt to add a worker fails with IllegalThreadStateException and the pool is unusable. In
 * that case this factory places the new thread in the ThreadGroup of the calling thread instead,
 * which is necessarily alive.
 * <p/>
 * ThreadGroups can be destroyed only up to JDK 18. From JDK 19, ThreadGroup.destroy() is a no-op
 * and the fallback is never used.
 */
public class ThreadGroupSafeThreadFactory implements ThreadFactory {
  private final ThreadFactory delegate = Executors.defaultThreadFactory();

  @Override
  public Thread newThread(Runnable r) {
    try {
      return delegate.newThread(r);
    } catch (IllegalThreadStateException e) {
      // The captured group is destroyed, use the group of the calling thread.
      Thread t = new Thread(r);
      if (t.isDaemon()) {
        t.setDaemon(false);
      }
      if (t.getPriority() != Thread.NORM_PRIORITY) {
        t.setPriority(Thread.NORM_PRIORITY);
      }
      return t;
    }
  }
}
