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
package org.apache.phoenix.hbase.index.covered.data;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;

import org.apache.phoenix.hbase.index.covered.update.ColumnReference;

/**
 * Access the current state of the row in the local HBase table, given a mutation
 */
public interface LocalHBaseState {

  /**
   * @param m mutation for which we should get the current table state
   * @param toCover all the columns the current row state needs to cover; hint the underlying lookup
   *          to save getting all the columns for the row
   * @return the full state of the given row. Includes all current versions (even if they are not
   *         usually visible to the client (unless they are also doing a raw scan)). Never returns a
   *         <tt>null</tt> {@link Result} - instead, when there is not data for the row, returns a
   *         {@link Result} with no stored {@link KeyValue}s.
   * @throws IOException if there is an issue reading the row
   */
  public Result getCurrentRowState(Mutation m, Collection<? extends ColumnReference> toCover)
      throws IOException;

}