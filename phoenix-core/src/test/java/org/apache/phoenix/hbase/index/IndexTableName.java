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

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Returns a {@code byte[]} containing the name of the currently running test method.
 */
public class IndexTableName extends TestWatcher {
  private String tableName;

  /**
   * Invoked when a test is about to start
   */
  @Override
  protected void starting(Description description) {
    tableName = description.getMethodName();
  }

  public byte[] getTableName() {
    return Bytes.toBytes(tableName);
  }

  public String getTableNameString() {
    return this.tableName;
  }
}