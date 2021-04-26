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

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;

public interface ValueGetter {
  public static final ImmutableBytesWritable HIDDEN_BY_DELETE = new ImmutableBytesWritable(new byte[0]);
  /**
   * Get the most recent (largest timestamp) for the given column reference
   * @param ref to match against an underlying key value. Uses the passed object to match the
   *          keyValue via {@link ColumnReference#matches}
 * @param ts time stamp at which mutations will be issued
   * @return the stored value for the given {@link ColumnReference}, <tt>null</tt> if no value is
   *         present, or {@link ValueGetter#HIDDEN_BY_DELETE} if no value is present and the ref
   *         will be shadowed by a delete marker.
   * @throws IOException if there is an error accessing the underlying data storage
   */
  public ImmutableBytesWritable getLatestValue(ColumnReference ref, long ts) throws IOException;
  public KeyValue getLatestKeyValue(ColumnReference ref, long ts) throws IOException;
  
  public byte[] getRowKey();

}