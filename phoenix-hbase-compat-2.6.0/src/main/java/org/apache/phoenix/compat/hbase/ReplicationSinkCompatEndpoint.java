/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.util.Optional;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;

/**
 * Replication Sink compat endpoint that helps attach WAL attributes to
 * mutation. In order to do so, this endpoint utilizes regionserver hook
 * {@link #preReplicationSinkBatchMutate(ObserverContext, AdminProtos.WALEntry, Mutation)}
 */
public class ReplicationSinkCompatEndpoint
        implements RegionServerCoprocessor, RegionServerObserver {

    @Override
    public Optional<RegionServerObserver> getRegionServerObserver() {
        return Optional.of(this);
    }

    @Override
    public void preReplicationSinkBatchMutate(
            ObserverContext<RegionServerCoprocessorEnvironment> ctx, AdminProtos.WALEntry walEntry,
            Mutation mutation) throws IOException {
        RegionServerObserver.super.preReplicationSinkBatchMutate(ctx, walEntry, mutation);
        List<WALProtos.Attribute> attributeList = walEntry.getKey().getExtendedAttributesList();
        attachWALExtendedAttributesToMutation(mutation, attributeList);
    }

    private void attachWALExtendedAttributesToMutation(Mutation mutation,
                                                       List<WALProtos.Attribute> attributeList) {
        if (attributeList != null) {
            for (WALProtos.Attribute attribute : attributeList) {
                mutation.setAttribute(attribute.getKey(), attribute.getValue().toByteArray());
            }
        }
    }
}
