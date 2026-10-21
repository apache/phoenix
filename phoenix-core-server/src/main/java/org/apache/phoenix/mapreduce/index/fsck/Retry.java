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
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs verification and repair steps, and retries a failed step with exponential backoff. Retry
 * immediately throws a failure that a retry cannot fix. Examples are a missing table, a permission
 * error, an invalid argument, and a {@link DoNotRetryIOException}. Retry also immediately throws an
 * interrupt, and keeps the interrupt status of the thread set. After the last retry, Retry throws
 * the last failure.
 */
public final class Retry {
  private static final Logger LOGGER = LoggerFactory.getLogger(Retry.class);

  static final int RETRIES = 4;
  static final long INITIAL_BACKOFF_MS = 1000;

  private Retry() {
  }

  public static <T> T call(String step, Callable<T> callable) throws Exception {
    return call(step, callable, INITIAL_BACKOFF_MS);
  }

  static <T> T call(String step, Callable<T> callable, long initialBackoffMs) throws Exception {
    RetryCounter retries = new RetryCounter(RETRIES, initialBackoffMs, TimeUnit.MILLISECONDS);
    while (true) {
      try {
        return callable.call();
      } catch (Exception e) {
        if (isInterrupt(e)) {
          Thread.currentThread().interrupt();
          throw e;
        }
        if (isUnrecoverable(e) || !retries.shouldRetry()) {
          throw e;
        }
        LOGGER.warn("{} failed, retry {} of {} in {} ms", step, retries.getAttemptTimes() + 1,
          RETRIES, retries.getBackoffTime(), e);
        try {
          retries.sleepUntilNextRetry();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw ie;
        }
      }
    }
  }

  public static void run(String step, RunnableStep runnable) throws Exception {
    call(step, () -> {
      runnable.run();
      return null;
    });
  }

  /** A step that returns no value and can throw any exception. */
  public interface RunnableStep {
    void run() throws Exception;
  }

  /**
   * Returns true if the failure is an interrupt, or if the thread has its interrupt status set. The
   * HBase rules apply: a socket timeout is not an interrupt, although it is an
   * {@link java.io.InterruptedIOException}.
   */
  static boolean isInterrupt(Throwable t) {
    for (Throwable cause = t; cause != null; cause = cause.getCause()) {
      if (ExceptionUtil.isInterrupt(cause)) {
        return true;
      }
    }
    return Thread.currentThread().isInterrupted();
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
