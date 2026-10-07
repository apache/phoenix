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
package org.apache.phoenix.mapreduce.index.fsck;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility for executing verification and repair operations with exponential backoff retry
 * semantics. Non-transient exceptions such as missing tables, permission errors, and
 * {@link DoNotRetryIOException} are propagated immediately without retry.
 */
public final class Retry {
  private static final Logger LOGGER = LoggerFactory.getLogger(Retry.class);

  static final int RETRIES = 4;
  static final long INITIAL_BACKOFF_MS = 1000;

  private Retry() {
  }

  public static <T> T call(String step, Callable<T> callable) throws Exception {
    RetryCounter retries = new RetryCounter(RETRIES, INITIAL_BACKOFF_MS, TimeUnit.MILLISECONDS);
    while (true) {
      try {
        return callable.call();
      } catch (Exception e) {
        if (isUnrecoverable(e) || !retries.shouldRetry()) {
          throw e;
        }
        LOGGER.warn("{} failed, retry {} of {} in {} ms", step, retries.getAttemptTimes() + 1,
          RETRIES, retries.getBackoffTime(), e);
        retries.sleepUntilNextRetry();
      }
    }
  }

  public static void run(String step, RunnableStep runnable) throws Exception {
    call(step, () -> {
      runnable.run();
      return null;
    });
  }

  /** Functional interface for a void execution step that may throw exceptions. */
  public interface RunnableStep {
    void run() throws Exception;
  }

  public static boolean isUnrecoverable(Throwable t) {
    for (Throwable cause = t; cause != null; cause = cause.getCause()) {
      if (
        cause instanceof org.apache.phoenix.schema.TableNotFoundException
          || cause instanceof org.apache.hadoop.hbase.TableNotFoundException
          || cause instanceof AccessDeniedException || cause instanceof IllegalArgumentException
          || cause instanceof UnsupportedOperationException
          || cause instanceof DoNotRetryIOException
      ) {
        return true;
      }
    }
    return false;
  }
}
