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
package org.apache.hadoop.hbase.ipc.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.DelegatingHBaseRpcController;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.phoenix.util.IndexUtil;

/**
 * Controller used to invalidate server side metadata cache RPCs.
 */
public class InvalidateMetadataCacheController extends DelegatingHBaseRpcController {
  private int priority;

  public InvalidateMetadataCacheController(HBaseRpcController delegate, Configuration conf) {
    super(delegate);
    this.priority = IndexUtil.getInvalidateMetadataCachePriority(conf);
  }

  @Override
  public void setPriority(int priority) {
    this.priority = priority;
  }

  @Override
  public void setPriority(TableName tn) {
    // Nothing
  }

  @Override
  public int getPriority() {
    return this.priority;
  }
}
