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

import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import com.google.common.collect.Multimap;;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;

import java.io.IOException;

/**
 * Write the index updates to the index tables
 */
public interface IndexCommitter extends Stoppable {

  void setup(IndexWriter parent, RegionCoprocessorEnvironment env, String name, boolean disableIndexOnFailure);

  public void write(Multimap<HTableInterfaceReference, Mutation> toWrite, boolean allowLocalUpdates, int clientVersion)
      throws IOException;
}