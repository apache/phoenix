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
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;

import com.google.common.collect.Multimap;

public class DelegateIndexFailurePolicy implements IndexFailurePolicy {

    private final IndexFailurePolicy delegate;
    
    public DelegateIndexFailurePolicy(IndexFailurePolicy delegate) {
        this.delegate = delegate;
    }

    @Override
    public void handleFailure(Multimap<HTableInterfaceReference, Mutation> attempted, Exception cause)
            throws IOException {
        delegate.handleFailure(attempted, cause);
    }

    @Override
    public boolean isStopped() {
        return delegate.isStopped();
    }

    @Override
    public void setup(Stoppable parent, RegionCoprocessorEnvironment env) {
        delegate.setup(parent, env);
    }

    @Override
    public void stop(String arg0) {
        delegate.stop(arg0);
    }

}
