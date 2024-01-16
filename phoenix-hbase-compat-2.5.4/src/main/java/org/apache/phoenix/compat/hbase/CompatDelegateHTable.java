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
package org.apache.phoenix.compat.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Table.CheckAndMutateBuilder;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

public abstract class CompatDelegateHTable implements Table {

    protected final Table delegate;

    public CompatDelegateHTable(Table delegate) {
        this.delegate = delegate;
    }

    @Override
    public RegionLocator getRegionLocator() throws IOException {
        return delegate.getRegionLocator();
    }

    @Override
    public Result mutateRow(RowMutations rm) throws IOException {
        return delegate.mutateRow(rm);
    }

    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
        return delegate.getTableDescriptor();
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
            byte[] value, Put put) throws IOException {
        return delegate.checkAndPut(row, family, qualifier, compareOp, value, put);
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
            byte[] value, Delete delete) throws IOException {
        return delegate.checkAndDelete(row, family, qualifier, compareOp, value, delete);
    }

    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
            byte[] value, RowMutations mutation) throws IOException {
        return delegate.checkAndMutate(row, family, qualifier, compareOp, value, mutation);
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
            throws IOException {
        return delegate.checkAndPut(row, family, qualifier, value, put);
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value,
            Delete delete) throws IOException {
        return delegate.checkAndDelete(row, family, qualifier, value, delete);
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
            byte[] value, Put put) throws IOException {
        return delegate.checkAndPut(row, family, qualifier, op, value, put);
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
            byte[] value, Delete delete) throws IOException {
        return delegate.checkAndDelete(row, family, qualifier, op, value, delete);
    }


    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
            byte[] value, RowMutations mutation) throws IOException {
        return delegate.checkAndMutate(row, family, qualifier, op, value, mutation);
    }

    @Override
    public CheckAndMutateBuilder checkAndMutate(byte[] row, byte[] family) {
        return delegate.checkAndMutate(row, family);
    }

    @Override
    public void setOperationTimeout(int operationTimeout) {
        delegate.setOperationTimeout(operationTimeout);
    }

    @Override
    public int getOperationTimeout() {
        return delegate.getOperationTimeout();
    }

    @Override
    public int getRpcTimeout() {
        return delegate.getRpcTimeout();
    }

    @Override
    public void setRpcTimeout(int rpcTimeout) {
        delegate.setRpcTimeout(rpcTimeout);
    }

    @Override
    public int getReadRpcTimeout() {
        return delegate.getReadRpcTimeout();
    }

    @Override
    public void setReadRpcTimeout(int readRpcTimeout) {
        delegate.setReadRpcTimeout(readRpcTimeout);
    }

    @Override
    public int getWriteRpcTimeout() {
        return delegate.getWriteRpcTimeout();
    }

    @Override
    public void setWriteRpcTimeout(int writeRpcTimeout) {
        delegate.setWriteRpcTimeout(writeRpcTimeout);
    }

    @Override
    public boolean[] existsAll(List<Get> gets) throws IOException {
        return delegate.existsAll(gets);
    }
}
