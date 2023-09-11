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
package org.apache.phoenix.hbase.index;

import org.apache.hadoop.hbase.Abortable;

/**
 * {@link Abortable} that can rethrow the cause of the abort.
 */
public class CapturingAbortable implements Abortable {

  private Abortable delegate;
  private Throwable cause;
  private String why;

  public CapturingAbortable(Abortable delegate) {
    this.delegate = delegate;
  }

  @Override
  public void abort(String why, Throwable e) {
    if (delegate.isAborted()) {
      return;
    }
    this.why = why;
    this.cause = e;
    delegate.abort(why, e);

  }

  @Override
  public boolean isAborted() {
    return delegate.isAborted();
  }

  /**
   * Throw the cause of the abort, if <tt>this</tt> was aborted. If there was an exception causing
   * the abort, re-throws that. Otherwise, just throws a generic {@link Exception} with the reason
   * why the abort was caused.
   * @throws Throwable the cause of the abort.
   */
  public void throwCauseIfAborted() throws Throwable {
    if (!this.isAborted()) {
      return;
    }
    if (cause == null) {
      throw new Exception(why);
    }
    throw cause;
  }
}