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
package org.apache.phoenix.hbase.index.write;

import java.io.IOException;

import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import com.google.common.collect.Multimap;

import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;

/**
 * Handle failures to write to the index tables.
 */
public interface IndexFailurePolicy extends Stoppable {

  public void setup(Stoppable parent, RegionCoprocessorEnvironment env);

  /**
   * Handle the failure of the attempted index updates
   * @param attempted map of index table -> mutations to apply
   * @param cause reason why there was a failure
 * @throws IOException 
   */
  public void
      handleFailure(Multimap<HTableInterfaceReference, Mutation> attempted, Exception cause) throws IOException;
}