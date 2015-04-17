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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.builder.BaseIndexCodec;
import org.apache.phoenix.hbase.index.covered.IndexCodec;
import org.apache.phoenix.hbase.index.covered.IndexMetaData;
import org.apache.phoenix.hbase.index.covered.IndexUpdate;
import org.apache.phoenix.hbase.index.covered.TableState;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.util.MetaDataUtil;

import com.google.common.collect.Lists;

/**
 * Phoenix-based {@link IndexCodec}. Manages all the logic of how to cleanup an index (
 * {@link #getIndexDeletes(TableState, IndexMetaData)}) as well as what the new index state should be (
 * {@link #getIndexUpserts(TableState, IndexMetaData)}).
 */
public class PhoenixIndexCodec extends BaseIndexCodec {
    public static final String INDEX_MD = "IdxMD";
    public static final String INDEX_UUID = "IdxUUID";
    public static final String INDEX_MAINTAINERS = "IndexMaintainers";
    private static KeyValueBuilder KV_BUILDER = GenericKeyValueBuilder.INSTANCE;

    private RegionCoprocessorEnvironment env;

    @Override
    public void initialize(RegionCoprocessorEnvironment env) throws IOException {
        super.initialize(env);
        this.env = env;
    }

    boolean hasIndexMaintainers(Map<String, byte[]> attributes) {
        if (attributes == null) { return false; }
        byte[] uuid = attributes.get(INDEX_UUID);
        if (uuid == null) { return false; }
        return true;
    }

    @Override
    public Iterable<IndexUpdate> getIndexUpserts(TableState state, IndexMetaData context) throws IOException {
        List<IndexMaintainer> indexMaintainers = ((PhoenixIndexMetaData)context).getIndexMaintainers();
        if (indexMaintainers.get(0).isRowDeleted(state.getPendingUpdate())) {
            return Collections.emptyList();
        }
        // TODO: confirm that this special case isn't needed
        // (as state should match this with the above call, since there are no mutable columns)
        /*
        if (maintainer.isImmutableRows()) {
            indexUpdate = new IndexUpdate(new ColumnTracker(maintainer.getAllColumns()));
            indexUpdate.setTable(maintainer.getIndexTableName());
            valueGetter = maintainer.createGetterFromKeyValues(dataRowKey, state.getPendingUpdate());
        }
        */
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(state.getCurrentRowKey());
        List<IndexUpdate> indexUpdates = Lists.newArrayList();
        for (IndexMaintainer maintainer : indexMaintainers) {
            if (maintainer.isLocalIndex()) { // TODO: remove this once verified assert passes
                assert(Bytes.compareTo(maintainer.getIndexTableName(), MetaDataUtil.getLocalIndexPhysicalName(env.getRegion().getTableDesc().getName())) == 0);
            }
            Pair<ValueGetter, IndexUpdate> statePair = state.getIndexUpdateState(maintainer.getAllColumns());
            ValueGetter valueGetter = statePair.getFirst();
            IndexUpdate indexUpdate = statePair.getSecond();
            indexUpdate.setTable(maintainer.getIndexTableName());
            Put put = maintainer.buildUpdateMutation(KV_BUILDER, valueGetter, ptr, state.getCurrentTimestamp(), env
                    .getRegion().getStartKey(), env.getRegion().getEndKey());
            if (put == null) {
                throw new IllegalStateException("Null put for " + env.getRegion().getRegionInfo().getTable().getNameAsString() 
                        + ": " + Bytes.toStringBinary(ptr.get(), ptr.getOffset(), ptr.getLength()));
            }
            indexUpdate.setUpdate(put);
            indexUpdates.add(indexUpdate);
        }
        return indexUpdates;
    }

    @Override
    public Iterable<IndexUpdate> getIndexDeletes(TableState state, IndexMetaData context) throws IOException {
        List<IndexMaintainer> indexMaintainers = ((PhoenixIndexMetaData)context).getIndexMaintainers();
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(state.getCurrentRowKey());
        List<IndexUpdate> indexUpdates = Lists.newArrayList();
        for (IndexMaintainer maintainer : indexMaintainers) {
            if (maintainer.isLocalIndex()) { // TODO: remove this once verified assert passes
                assert(Bytes.compareTo(maintainer.getIndexTableName(), MetaDataUtil.getLocalIndexPhysicalName(env.getRegion().getTableDesc().getName())) == 0);
            }
            if (maintainer.isImmutableRows()) {
                continue;
            }
            Pair<ValueGetter, IndexUpdate> statePair = state.getIndexUpdateState(maintainer.getAllColumns());
            ValueGetter valueGetter = statePair.getFirst();
            IndexUpdate indexUpdate = statePair.getSecond();
            indexUpdate.setTable(maintainer.getIndexTableName());
            Delete delete = maintainer.buildDeleteMutation(KV_BUILDER, valueGetter, ptr, state.getPendingUpdate(),
                    state.getCurrentTimestamp(), env.getRegion().getStartKey(), env.getRegion().getEndKey());
            indexUpdate.setUpdate(delete);
            indexUpdates.add(indexUpdate);
        }
        return indexUpdates;
    }

    /*
    @Override
    public Iterable<IndexUpdate> getIndexUpserts(TableState state, BatchContext context) throws IOException {
        return getIndexUpdates(state, context, true);
    }

    @Override
    public Iterable<IndexUpdate> getIndexDeletes(TableState state, BatchContext context) throws IOException {
        return getIndexUpdates(state, context, false);
    }

    private Iterable<IndexUpdate> getIndexUpdates(TableState state, BatchContext context, boolean upsert) throws IOException {
        List<IndexMaintainer> indexMaintainers = ((PhoenixBatchContext)context).getIndexMetaData().getIndexMaintainers();
        if (indexMaintainers.isEmpty()) { return Collections.emptyList(); }
        List<IndexUpdate> indexUpdates = Lists.newArrayList();
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        // TODO: state.getCurrentRowKey() should take an ImmutableBytesWritable arg to prevent byte copy
        byte[] dataRowKey = state.getCurrentRowKey();
        ptr.set(dataRowKey);
        byte[] localIndexTableName = MetaDataUtil.getLocalIndexPhysicalName(env.getRegion().getTableDesc().getName());
        ValueGetter valueGetter = null;
        Scanner scanner = null;
        for (IndexMaintainer maintainer : indexMaintainers) {
            if (upsert) {
                // Short-circuit building state when we know it's a row deletion
                if (maintainer.isRowDeleted(state.getPendingUpdate())) {
                    continue;
                }
            }
            IndexUpdate indexUpdate = null;
            if (maintainer.isImmutableRows()) {
                indexUpdate = new IndexUpdate(new ColumnTracker(maintainer.getAllColumns()));
                if (maintainer.isLocalIndex()) {
                    indexUpdate.setTable(localIndexTableName);
                } else {
                    indexUpdate.setTable(maintainer.getIndexTableName());
                }
                valueGetter = maintainer.createGetterFromKeyValues(dataRowKey, state.getPendingUpdate());
            } else {
                Pair<ValueGetter, IndexUpdate> statePair = state.getIndexUpdateState(maintainer.getAllColumns());
                valueGetter = statePair.getFirst();
                indexUpdate = statePair.getSecond();
                indexUpdate.setTable(maintainer.getIndexTableName());
            }
            Mutation mutation = null;
            if (upsert) {
                mutation = maintainer.buildUpdateMutation(KV_BUILDER, valueGetter, ptr, state.getCurrentTimestamp(), env
                        .getRegion().getStartKey(), env.getRegion().getEndKey());
            } else {
                mutation = maintainer.buildDeleteMutation(KV_BUILDER, valueGetter, ptr, state.getPendingUpdate(),
                        state.getCurrentTimestamp(), env.getRegion().getStartKey(), env.getRegion().getEndKey());
            }
            indexUpdate.setUpdate(mutation);
            if (scanner != null) {
                scanner.close();
                scanner = null;
            }
            indexUpdates.add(indexUpdate);
        }
        return indexUpdates;
    }
    */

    @Override
    public boolean isEnabled(Mutation m) throws IOException {
        return hasIndexMaintainers(m.getAttributesMap());
    }
}