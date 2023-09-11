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
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.hbase.index.BaseIndexCodec;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.IndexCodec;
import org.apache.phoenix.hbase.index.covered.IndexMetaData;
import org.apache.phoenix.hbase.index.covered.IndexUpdate;
import org.apache.phoenix.hbase.index.covered.TableState;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;

import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.DO_TRANSFORMING;

/**
 * Phoenix-based {@link IndexCodec}. Manages all the logic of how to cleanup an index (
 * {@link #getIndexDeletes(TableState, IndexMetaData, byte[], byte[])}) as well as what the new index state should be (
 * {@link #getIndexUpserts(TableState, IndexMetaData, byte[], byte[])}).
 */
public class PhoenixIndexCodec extends BaseIndexCodec {
    public static final String INDEX_MD = "IdxMD";
    public static final String INDEX_PROTO_MD = "IdxProtoMD";
    public static final String INDEX_UUID = "IdxUUID";
    public static final String INDEX_MAINTAINERS = "IndexMaintainers";
    public static final String INDEX_NAME_FOR_IDX_MAINTAINER = "INDEX_IDX_MAINTAINER";
    public static final KeyValueBuilder KV_BUILDER = GenericKeyValueBuilder.INSTANCE;
    
    private byte[] tableName;
    
    public PhoenixIndexCodec() {
        
    }

    public PhoenixIndexCodec(Configuration conf, byte[] tableName) {
        initialize(conf, tableName);
    }
    

    @Override
    public void initialize(Configuration conf, byte[] tableName) {
        this.tableName = tableName;
    }

    boolean hasIndexMaintainers(Map<String, byte[]> attributes) {
        if (attributes == null) { return false; }
        byte[] uuid = attributes.get(INDEX_UUID);
        if (uuid == null) { return false; }
        return true;
    }

    boolean isTransforming(Map<String, byte[]> attributes) {
        if (attributes == null) { return false; }
        byte[] transforming = attributes.get(DO_TRANSFORMING);
        if (transforming == null) { return false; }
        return true;
    }

    @Override
    public Iterable<IndexUpdate> getIndexUpserts(
            TableState state, IndexMetaData context, byte[] regionStartKey, byte[] regionEndKey,
            boolean verified) throws IOException {
        PhoenixIndexMetaData metaData = (PhoenixIndexMetaData)context;
        List<IndexMaintainer> indexMaintainers = metaData.getIndexMaintainers();
        if (indexMaintainers.get(0).isRowDeleted(state.getPendingUpdate())) {
            return Collections.emptyList();
        }
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(state.getCurrentRowKey());
        List<IndexUpdate> indexUpdates = Lists.newArrayList();
        for (IndexMaintainer maintainer : indexMaintainers) {
            Pair<ValueGetter, IndexUpdate> statePair = state.getIndexUpdateState(maintainer.getAllColumns(), metaData.getReplayWrite() != null, false, context);
            ValueGetter valueGetter = statePair.getFirst();
            IndexUpdate indexUpdate = statePair.getSecond();
            indexUpdate.setTable(maintainer.isLocalIndex() ? tableName : maintainer.getIndexTableName());
            Put put = maintainer.buildUpdateMutation(KV_BUILDER, valueGetter, ptr, state.getCurrentTimestamp(),
                    regionStartKey, regionEndKey, verified);
            indexUpdate.setUpdate(put);
            indexUpdates.add(indexUpdate);
        }
        return indexUpdates;
    }

    @Override
    public Iterable<IndexUpdate> getIndexDeletes(TableState state, IndexMetaData context, byte[] regionStartKey, byte[] regionEndKey) throws IOException {
        PhoenixIndexMetaData metaData = (PhoenixIndexMetaData)context;
        List<IndexMaintainer> indexMaintainers = metaData.getIndexMaintainers();
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(state.getCurrentRowKey());
        List<IndexUpdate> indexUpdates = Lists.newArrayList();
        for (IndexMaintainer maintainer : indexMaintainers) {
            // For transactional tables, we use an index maintainer
            // to aid in rollback if there's a KeyValue column in the index. The alternative would be
            // to hold on to all uncommitted index row keys (even ones already sent to HBase) on the
            // client side.
            Set<ColumnReference> cols = Sets.newHashSet(maintainer.getAllColumns());
            cols.add(new ColumnReference(indexMaintainers.get(0).getDataEmptyKeyValueCF(), indexMaintainers.get(0).getEmptyKeyValueQualifier()));
            Pair<ValueGetter, IndexUpdate> statePair = state.getIndexUpdateState(cols, metaData.getReplayWrite() != null, true, context);
            ValueGetter valueGetter = statePair.getFirst();
            if (valueGetter!=null) {
                IndexUpdate indexUpdate = statePair.getSecond();
                indexUpdate.setTable(maintainer.isLocalIndex() ? tableName : maintainer.getIndexTableName());
                Delete delete = maintainer.buildDeleteMutation(KV_BUILDER, valueGetter, ptr, state.getPendingUpdate(),
                        state.getCurrentTimestamp(), regionStartKey, regionEndKey);
                indexUpdate.setUpdate(delete);
                indexUpdates.add(indexUpdate);
            }
        }
        return indexUpdates;
    }

    @Override
    public boolean isEnabled(Mutation m) {
        return hasIndexMaintainers(m.getAttributesMap()) || isTransforming(m.getAttributesMap());
    }
}
