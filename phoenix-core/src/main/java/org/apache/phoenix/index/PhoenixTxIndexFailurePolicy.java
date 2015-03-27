/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.index;

import java.io.IOException;

import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.write.IndexFailurePolicy;

import com.google.common.collect.Multimap;

public class PhoenixTxIndexFailurePolicy implements IndexFailurePolicy {
    private Stoppable parent;

    @Override
    public void stop(String why) {
        if (parent != null) {
            parent.stop(why);
        }
    }

    @Override
    public boolean isStopped() {
        return parent == null ? false : parent.isStopped();
    }

    @Override
    public void setup(Stoppable parent, RegionCoprocessorEnvironment env) {
        this.parent = parent;
    }

    @Override
    public void handleFailure(Multimap<HTableInterfaceReference, Mutation> attempted, Exception cause)
            throws IOException {
        if (cause instanceof IOException) {
            throw (IOException)cause;
        } else if (cause instanceof RuntimeException) { throw (RuntimeException)cause; }
        throw new IOException(cause);
    }
}
