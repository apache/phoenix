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
package org.apache.phoenix.hbase.index.parallel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Helper utility to make a thread pool from a configuration based on reasonable defaults and passed
 * configuration keys.
 */
public class ThreadPoolBuilder {

  private static final Log LOG = LogFactory.getLog(ThreadPoolBuilder.class);
  private static final long DEFAULT_TIMEOUT = 60;
  private static final int DEFAULT_MAX_THREADS = 1;// is there a better default?
  private Pair<String, Long> timeout;
  private Pair<String, Integer> maxThreads;
  private String name;
  private Configuration conf;

  public ThreadPoolBuilder(String poolName, Configuration conf) {
    this.name = poolName;
    this.conf = conf;
  }

  public ThreadPoolBuilder setCoreTimeout(String confkey, long defaultTime) {
    if (defaultTime <= 0) {
      defaultTime = DEFAULT_TIMEOUT;
    }
    this.timeout = new Pair<String, Long>(confkey, defaultTime);
    return this;
  }

  public ThreadPoolBuilder setCoreTimeout(String confKey) {
    return this.setCoreTimeout(confKey, DEFAULT_TIMEOUT);
  }

  public ThreadPoolBuilder setMaxThread(String confkey, int defaultThreads) {
    if (defaultThreads <= 0) {
      defaultThreads = DEFAULT_MAX_THREADS;
    }
    this.maxThreads = new Pair<String, Integer>(confkey, defaultThreads);
    return this;
  }

  String getName() {
   return this.name;
  }

  int getMaxThreads() {
    int maxThreads = DEFAULT_MAX_THREADS;
    if (this.maxThreads != null) {
      String key = this.maxThreads.getFirst();
      maxThreads =
          key == null ? this.maxThreads.getSecond() : conf.getInt(key, this.maxThreads.getSecond());
    }
    LOG.trace("Creating pool builder with max " + maxThreads + " threads ");
    return maxThreads;
  }

  long getKeepAliveTime() {
    long timeout =DEFAULT_TIMEOUT;
    if (this.timeout != null) {
      String key = this.timeout.getFirst();
      timeout =
          key == null ? this.timeout.getSecond() : conf.getLong(key, this.timeout.getSecond());
    }

    LOG.trace("Creating pool builder with core thread timeout of " + timeout + " seconds ");
    return timeout;
  }
}