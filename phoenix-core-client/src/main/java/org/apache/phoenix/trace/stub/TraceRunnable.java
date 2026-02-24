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
package org.apache.phoenix.trace.stub;

/**
 * Stub class for HTrace TraceRunnable. This is a no-op replacement for
 * {@code org.apache.htrace.wrappers.TraceRunnable} to remove the HTrace dependency.
 * Simply delegates to the wrapped runnable.
 */
public class TraceRunnable implements Runnable {

  private final Runnable runnable;

  public TraceRunnable(Span parent, Runnable runnable, String description) {
    this.runnable = runnable;
  }

  @Override
  public void run() {
    runnable.run();
  }
}
