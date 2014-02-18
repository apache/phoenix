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

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;

import org.apache.phoenix.hbase.index.TableName;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolBuilder;

public class TestThreadPoolBuilder {

  @Rule
  public TableName name = new TableName();

  @Test
  public void testCoreThreadTimeoutNonZero() {
    Configuration conf = new Configuration(false);
    String key = name.getTableNameString()+"-key";
    ThreadPoolBuilder builder = new ThreadPoolBuilder(name.getTableNameString(), conf);
    assertTrue("core threads not set, but failed return", builder.getKeepAliveTime() > 0);
    // set an negative value
    builder.setCoreTimeout(key, -1);
    assertTrue("core threads not set, but failed return", builder.getKeepAliveTime() > 0);
    // set a positive value
    builder.setCoreTimeout(key, 1234);
    assertEquals("core threads not set, but failed return", 1234, builder.getKeepAliveTime());
    // set an empty value
    builder.setCoreTimeout(key);
    assertTrue("core threads not set, but failed return", builder.getKeepAliveTime() > 0);
  }
  
  @Test
  public void testMaxThreadsNonZero() {
    Configuration conf = new Configuration(false);
    String key = name.getTableNameString()+"-key";
    ThreadPoolBuilder builder = new ThreadPoolBuilder(name.getTableNameString(), conf);
    assertTrue("core threads not set, but failed return", builder.getMaxThreads() > 0);
    // set an negative value
    builder.setMaxThread(key, -1);
    assertTrue("core threads not set, but failed return", builder.getMaxThreads() > 0);
    // set a positive value
    builder.setMaxThread(key, 1234);
    assertEquals("core threads not set, but failed return", 1234, builder.getMaxThreads());
  }
}