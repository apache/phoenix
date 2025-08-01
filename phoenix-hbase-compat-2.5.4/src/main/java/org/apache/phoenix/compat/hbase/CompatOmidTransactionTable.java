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
package org.apache.phoenix.compat.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

public abstract class CompatOmidTransactionTable implements Table {

  @Override
  public RegionLocator getRegionLocator() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Result mutateRow(RowMutations rm) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
    byte[] value, Put put) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
    byte[] value, Delete delete) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
    byte[] value, RowMutations mutation) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CheckAndMutateBuilder checkAndMutate(byte[] row, byte[] family) {
    throw new UnsupportedOperationException();
  }
}
