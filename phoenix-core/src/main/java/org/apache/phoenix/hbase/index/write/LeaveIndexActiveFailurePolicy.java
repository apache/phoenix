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
import org.apache.phoenix.util.ServerUtil;

import com.google.common.collect.Multimap;

/**
 * 
 * Implementation of IndexFailurePolicy which takes no action when an
 * index cannot be updated. As with the standard flow of control, an
 * exception will still be thrown back to the client. Using this failure
 * policy means that the action to take upon failure is completely up
 * to the client.
 *
 */
public class LeaveIndexActiveFailurePolicy implements IndexFailurePolicy {

    @Override
    public boolean isStopped() {
        return false;
    }

    @Override
    public void stop(String arg0) {
    }

    @Override
    public void setup(Stoppable parent, RegionCoprocessorEnvironment env) {
    }

    @Override
    public void handleFailure(Multimap<HTableInterfaceReference, Mutation> attempted, Exception cause)
            throws IOException {
        // get timestamp of first cell
        long ts = attempted.values().iterator().next().getFamilyCellMap().values().iterator().next().get(0).getTimestamp();
        throw ServerUtil.wrapInDoNotRetryIOException("Unable to update the following indexes: " + attempted.keySet(), cause, ts);
    }

}
